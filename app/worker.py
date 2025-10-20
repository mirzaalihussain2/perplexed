import asyncio
import logging
import os
import tempfile
import httpx
from arq import create_pool
from arq.connections import RedisSettings
from config import Config
import redis.asyncio as redis
from supabase import create_client
from elevenlabs import ElevenLabs
from app.common.storage import upload_to_supabase
from app.common.video import split_video as split_video_ffmpeg
from app.common.perplexity import process_transcript_references

logging.basicConfig(level=logging.INFO, format='%(message)s')

def get_supabase_client():
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

def get_elevenlabs_client():
    return ElevenLabs(api_key=Config.ELEVENLABS_API_KEY)

async def get_redis_client():
    return redis.Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        db=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        ssl=Config.REDIS_SSL,
        ssl_cert_reqs=None,
        socket_connect_timeout=30,
        socket_timeout=30,
        decode_responses=True,
        max_connections=5
    )

async def download_video_from_url(url: str, output_path: str):
    """Download video/image from URL to local path"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        
        # Log the actual content type received
        content_type = response.headers.get('content-type', 'unknown')
        logging.info(f"[download] Downloaded from {url[:80]}... Content-Type: {content_type}, Size: {len(response.content)} bytes")
        
        with open(output_path, 'wb') as f:
            f.write(response.content)

async def transcribe_with_elevenlabs(video_path: str, max_retries: int = 2) -> str:
    """
    Transcribe audio using ElevenLabs Speech-to-Text API.
    Uses local file path with retry logic.
    """
    client = get_elevenlabs_client()
    
    for attempt in range(max_retries):
        try:
            with open(video_path, 'rb') as f:
                result = client.speech_to_text.convert(
                    model_id="scribe_v1",
                    file=f,
                    language_code="en"
                )
            return result.text
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"[transcribe] Attempt {attempt + 1} failed: {e}, retrying...")
                await asyncio.sleep(2)
            else:
                logging.error(f"[transcribe] All attempts failed: {e}")
                raise

async def split_video(ctx, job_id: str, chunk_duration: int = 20, max_duration: int = 40):
    """
    Split video into chunks, upload to Supabase, enqueue process_clip tasks.
    
    Args:
        job_id: Job identifier
        chunk_duration: Duration of each chunk in seconds (default 20)
        max_duration: Maximum video duration to process in seconds (default 120 = 2 minutes)
    """
    redis_client = None
    temp_video = None
    temp_chunks_dir = None
    
    try:
        logging.info(f"[split_video] Starting job {job_id} (chunk_duration={chunk_duration}s, max_duration={max_duration}s)")
        
        redis_client = await get_redis_client()
        await redis_client.set(f"job:{job_id}:status", "processing")
        
        video_url = await redis_client.get(f"job:{job_id}:video_path")
        if not video_url:
            raise ValueError(f"No video_path found for job {job_id}")
        
        logging.info(f"[split_video] Downloading video from {video_url}")
        
        temp_video = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
        await download_video_from_url(video_url, temp_video)
        
        if max_duration:
            logging.info(f"[split_video] Trimming video to first {max_duration}s")
            trimmed_video = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
            import subprocess
            subprocess.run([
                'ffmpeg', '-i', temp_video, '-t', str(max_duration),
                '-c:v', 'libx264',  # Re-encode video instead of copy
                '-c:a', 'aac',       # Re-encode audio instead of copy
                '-y', trimmed_video
            ], check=True, capture_output=True)
            os.remove(temp_video)
            temp_video = trimmed_video
        
        logging.info(f"[split_video] Splitting video into {chunk_duration}s chunks")
        temp_chunks_dir = tempfile.mkdtemp()
        chunk_paths = split_video_ffmpeg(temp_video, temp_chunks_dir, clip_duration=chunk_duration)
        
        num_chunks = len(chunk_paths)
        logging.info(f"[split_video] Created {num_chunks} chunks, uploading to Supabase")
        
        supabase = get_supabase_client()
        for idx, chunk_path in enumerate(chunk_paths):
            remote_path = f"videos/{job_id}/chunks/{idx}.mp4"
            upload_to_supabase(supabase, Config.SUPABASE_BUCKET, chunk_path, remote_path)
            logging.info(f"[split_video] Uploaded chunk {idx}/{num_chunks}")
        
        await redis_client.set(f"job:{job_id}:total", str(num_chunks))
        
        for idx in range(num_chunks):
            await ctx['pool'].enqueue_job('process_clip', job_id, idx)
        
        logging.info(f"[split_video] Job {job_id} complete, enqueued {num_chunks} clip tasks")
        
    except Exception as e:
        logging.error(f"[split_video] Job {job_id} failed: {e}")
        if redis_client:
            await redis_client.set(f"job:{job_id}:status", "failed")
            await redis_client.set(f"job:{job_id}:error", str(e))
        raise
    
    finally:
        if redis_client:
            await redis_client.close()
        
        if temp_video and os.path.exists(temp_video):
            os.remove(temp_video)
        
        if temp_chunks_dir and os.path.exists(temp_chunks_dir):
            for file in os.listdir(temp_chunks_dir):
                os.remove(os.path.join(temp_chunks_dir, file))
            os.rmdir(temp_chunks_dir)

async def process_clip(ctx, job_id: str, idx: int):
    logging.info(f"[process_clip] Starting clip {idx} for job {job_id}")
    
    redis_client = None
    supabase = get_supabase_client()
    
    temp_clip = None
    temp_audio = None
    temp_image = None
    temp_replacement = None
    
    try:
        redis_client = await get_redis_client()
        clip_remote_path = f"videos/{job_id}/chunks/{idx}.mp4"
        temp_clip = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
        
        logging.info(f"[process_clip] Downloading clip {idx}")
        clip_data = supabase.storage.from_(Config.SUPABASE_BUCKET).download(clip_remote_path)
        with open(temp_clip, 'wb') as f:
            f.write(clip_data)
        
        # Step 1: Transcribe audio with ElevenLabs
        try:
            logging.info(f"[process_clip] Transcribing clip {idx}")
            transcript = await transcribe_with_elevenlabs(temp_clip)
            logging.info(f"[process_clip] Transcript: {transcript[:100]}...")
        except Exception as e:
            logging.error(f"[process_clip] Transcription failed for clip {idx}: {e}")
            logging.info(f"[process_clip] Skipping clip {idx}, using original")
            await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
            await redis_client.set(f"job:{job_id}:clip:{idx}:error", "transcription_failed")
            
            # Still increment done count
            done = await redis_client.incr(f"job:{job_id}:done")
            total = int(await redis_client.get(f"job:{job_id}:total") or 0)
            logging.info(f"[process_clip] Job {job_id}: {done}/{total} clips done")
            
            if done == total:
                await ctx['pool'].enqueue_job('stitch_video', job_id)
                logging.info(f"[process_clip] All clips done, enqueuing stitch_video")
            return
        
        # Step 2: Extract references using Perplexity
        try:
            logging.info(f"[process_clip] Extracting references from transcript")
            perplexity_results = process_transcript_references(transcript)
            
            # Find first available image URL from any category
            image_url = None
            reference_name = None
            
            for person in perplexity_results.get('people', []):
                if person.get('image_url'):
                    image_url = person['image_url']
                    reference_name = person['name']
                    logging.info(f"[process_clip] Found person reference: {reference_name}")
                    break
            
            if not image_url:
                for org in perplexity_results.get('organisations', []):
                    if org.get('image_url'):
                        image_url = org['image_url']
                        reference_name = org['name']
                        logging.info(f"[process_clip] Found organisation reference: {reference_name}")
                        break
            
            if not image_url:
                for content in perplexity_results.get('content', []):
                    if content.get('image_url'):
                        image_url = content['image_url']
                        reference_name = f"{content['description']} ({content['type']})"
                        logging.info(f"[process_clip] Found content reference: {reference_name}")
                        break
            
            if not image_url:
                for event in perplexity_results.get('events', []):
                    if event.get('image_url'):
                        image_url = event['image_url']
                        reference_name = event['description']
                        logging.info(f"[process_clip] Found event reference: {reference_name}")
                        break
        except Exception as e:
            logging.error(f"[process_clip] Perplexity search failed for clip {idx}: {e}")
            image_url = None
        
        # Step 3: If we found a reference with an image, create replacement
        if image_url:
            # Check if this image URL has already been used
            used_images_key = f"job:{job_id}:used_images"
            used_images = await redis_client.smembers(used_images_key)
            
            if image_url in used_images:
                logging.info(f"[process_clip] Image {image_url[:50]}... already used, skipping clip {idx}")
                await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
            else:
                try:
                    logging.info(f"[process_clip] Creating replacement with image: {image_url}")
                    
                    # Mark this image as used
                    await redis_client.sadd(used_images_key, image_url)
                    
                    # Upload the image URL reference to Supabase for debugging
                    logging.info(f"[process_clip] Saving image reference for clip {idx}")
                    image_ref_path = f"videos/{job_id}/images/{idx}_reference.txt"
                    temp_ref = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False).name
                    with open(temp_ref, 'w') as f:
                        f.write(f"Reference: {reference_name}\nImage URL: {image_url}")
                    upload_to_supabase(supabase, Config.SUPABASE_BUCKET, temp_ref, image_ref_path)
                    os.remove(temp_ref)
                    logging.info(f"[process_clip] Saved reference to {image_ref_path}")
                    
                    # Download image
                    temp_image = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False).name
                    logging.info(f"[process_clip] Downloading image from {image_url}")
                    await download_video_from_url(image_url, temp_image)
                    logging.info(f"[process_clip] Image downloaded successfully")
                    
                    # Upload the actual image to Supabase for debugging
                    image_backup_path = f"videos/{job_id}/images/{idx}_image.jpg"
                    upload_to_supabase(supabase, Config.SUPABASE_BUCKET, temp_image, image_backup_path, content_type='image/jpeg')
                    logging.info(f"[process_clip] Backed up image to {image_backup_path}")
                    
                    # Extract audio from clip
                    temp_audio = tempfile.NamedTemporaryFile(suffix='.mp3', delete=False).name
                    from app.common.video import extract_audio, create_video_from_image_and_audio
                    extract_audio(temp_clip, temp_audio)
                    
                    # Create replacement video (image + audio)
                    temp_replacement = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
                    create_video_from_image_and_audio(temp_image, temp_audio, temp_replacement)
                    
                    # Upload replacement to Supabase
                    replacement_remote_path = f"videos/{job_id}/replacements/{idx}.mp4"
                    upload_to_supabase(supabase, Config.SUPABASE_BUCKET, temp_replacement, replacement_remote_path)
                    logging.info(f"[process_clip] Uploaded replacement for clip {idx}")
                    
                    await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "true")
                except Exception as e:
                    logging.error(f"[process_clip] Failed to create replacement for clip {idx}: {e}")
                    logging.info(f"[process_clip] Using original clip {idx}")
                    await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
                    await redis_client.set(f"job:{job_id}:clip:{idx}:error", "replacement_failed")
        else:
            logging.info(f"[process_clip] No reference with image found for clip {idx}, using original")
            await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
        
        # Atomic increment and check if all done
        done = await redis_client.incr(f"job:{job_id}:done")
        total = int(await redis_client.get(f"job:{job_id}:total") or 0)
        
        logging.info(f"[process_clip] Job {job_id}: {done}/{total} clips done")
        
        if done == total:
            await ctx['pool'].enqueue_job('stitch_video', job_id)
            logging.info(f"[process_clip] All clips done, enqueuing stitch_video")
    
    except Exception as e:
        logging.error(f"[process_clip] Job {job_id}, clip {idx} failed critically: {e}")
        # Still try to increment done count so job doesn't hang
        if redis_client:
            try:
                await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
                await redis_client.set(f"job:{job_id}:clip:{idx}:error", "critical_failure")
                done = await redis_client.incr(f"job:{job_id}:done")
                total = int(await redis_client.get(f"job:{job_id}:total") or 0)
                logging.info(f"[process_clip] Job {job_id}: {done}/{total} clips done (after error)")
                if done == total:
                    await ctx['pool'].enqueue_job('stitch_video', job_id)
            except Exception as redis_error:
                logging.error(f"[process_clip] Failed to update Redis after error: {redis_error}")
    
    finally:
        if redis_client:
            await redis_client.close()
        
        for temp_file in [temp_clip, temp_audio, temp_image, temp_replacement]:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)

async def stitch_video(ctx, job_id: str):
    logging.info(f"[stitch_video] Starting job {job_id}")
    
    redis_client = None
    supabase = get_supabase_client()
    temp_concat_dir = None
    final_video = None
    
    try:
        redis_client = await get_redis_client()
        await redis_client.set(f"job:{job_id}:status", "stitching")
        
        total = int(await redis_client.get(f"job:{job_id}:total") or 0)
        logging.info(f"[stitch_video] Stitching {total} clips")
        
        temp_concat_dir = tempfile.mkdtemp()
        clip_paths = []
        
        for idx in range(total):
            has_replacement = await redis_client.get(f"job:{job_id}:clip:{idx}:has_replacement")
            
            if has_replacement == "true":
                remote_path = f"videos/{job_id}/replacements/{idx}.mp4"
                logging.info(f"[stitch_video] Using replacement for clip {idx}")
            else:
                remote_path = f"videos/{job_id}/chunks/{idx}.mp4"
                logging.info(f"[stitch_video] Using original for clip {idx}")
            
            temp_clip = os.path.join(temp_concat_dir, f"clip_{idx:04d}.mp4")
            clip_data = supabase.storage.from_(Config.SUPABASE_BUCKET).download(remote_path)
            with open(temp_clip, 'wb') as f:
                f.write(clip_data)
            clip_paths.append(temp_clip)
        
        logging.info(f"[stitch_video] Concatenating {len(clip_paths)} clips")
        final_video = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
        from app.common.video import concat_videos
        concat_videos(clip_paths, final_video)
        
        final_remote_path = f"videos/{job_id}/final.mp4"
        upload_to_supabase(supabase, Config.SUPABASE_BUCKET, final_video, final_remote_path)
        logging.info(f"[stitch_video] Uploaded final video to {final_remote_path}")
        
        final_url = supabase.storage.from_(Config.SUPABASE_BUCKET).get_public_url(final_remote_path)
        await redis_client.set(f"job:{job_id}:final_url", final_url)
        await redis_client.set(f"job:{job_id}:status", "finished")
        
        logging.info(f"[stitch_video] Job {job_id} finished! Final URL: {final_url}")
        
    except Exception as e:
        logging.error(f"[stitch_video] Job {job_id} failed: {e}")
        if redis_client:
            await redis_client.set(f"job:{job_id}:status", "failed")
            await redis_client.set(f"job:{job_id}:error", str(e))
        raise
    
    finally:
        if redis_client:
            await redis_client.close()
        
        if final_video and os.path.exists(final_video):
            os.remove(final_video)
        
        if temp_concat_dir and os.path.exists(temp_concat_dir):
            for file in os.listdir(temp_concat_dir):
                os.remove(os.path.join(temp_concat_dir, file))
            os.rmdir(temp_concat_dir)

async def startup(ctx):
    redis_settings = RedisSettings(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        database=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        ssl=Config.REDIS_SSL,
        ssl_cert_reqs=None
    )
    ctx['pool'] = await create_pool(redis_settings)

async def shutdown(ctx):
    await ctx['pool'].close()

class WorkerSettings:
    redis_settings = RedisSettings(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        database=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        ssl=Config.REDIS_SSL,
        ssl_cert_reqs=None
    )
    functions = [split_video, process_clip, stitch_video]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 1
    job_timeout = 300  # 5 minutes per job