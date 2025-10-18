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
from app.common.storage import upload_to_supabase
from app.common.video import split_video as split_video_ffmpeg

logging.basicConfig(level=logging.INFO, format='%(message)s')

def get_supabase_client():
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)

async def get_redis_client():
    return redis.Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        db=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        ssl=Config.REDIS_SSL,
        ssl_cert_reqs=None,
        socket_connect_timeout=10,
        socket_timeout=10,
        decode_responses=True
    )

async def download_video_from_url(url: str, output_path: str):
    """Download video from URL to local path"""
    async with httpx.AsyncClient() as client:
        response = await client.get(url, follow_redirects=True)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            f.write(response.content)

async def split_video(ctx, job_id: str, chunk_duration: int = 20):
    """
    Split video into chunks, upload to Supabase, enqueue process_clip tasks.
    
    Args:
        job_id: Job identifier
        chunk_duration: Duration of each chunk in seconds (default 20)
    """
    redis_client = None
    temp_video = None
    temp_chunks_dir = None
    
    try:
        logging.info(f"[split_video] Starting job {job_id} (chunk_duration={chunk_duration}s)")
        
        redis_client = await get_redis_client()
        await redis_client.set(f"job:{job_id}:status", "processing")
        
        video_url = await redis_client.get(f"job:{job_id}:video_path")
        if not video_url:
            raise ValueError(f"No video_path found for job {job_id}")
        
        logging.info(f"[split_video] Downloading video from {video_url}")
        
        temp_video = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
        await download_video_from_url(video_url, temp_video)
        
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
    """
    Process a single clip: transcribe → check Perplexity → optionally create replacement.
    """
    redis_client = None
    temp_clip = None
    temp_audio = None
    temp_image = None
    temp_replacement = None
    
    try:
        logging.info(f"[process_clip] Starting job {job_id}, clip {idx}")
        
        redis_client = await get_redis_client()
        supabase = get_supabase_client()
        
        # Download clip from Supabase
        clip_remote_path = f"videos/{job_id}/chunks/{idx}.mp4"
        temp_clip = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False).name
        
        logging.info(f"[process_clip] Downloading clip {idx}")
        clip_data = supabase.storage.from_(Config.SUPABASE_BUCKET).download(clip_remote_path)
        with open(temp_clip, 'wb') as f:
            f.write(clip_data)
        
        # Step 1: Transcribe audio (MOCKED)
        logging.info(f"[process_clip] Transcribing clip {idx}")
        transcript = await mock_transcribe(temp_clip)
        logging.info(f"[process_clip] Transcript: {transcript[:100]}...")
        
        # Step 2: Check Perplexity for references (MOCKED)
        logging.info(f"[process_clip] Checking Perplexity for references")
        perplexity_result = await mock_perplexity_check(transcript)
        
        if perplexity_result and perplexity_result.get('has_reference'):
            image_url = perplexity_result.get('image_url')
            logging.info(f"[process_clip] Reference found with image: {image_url}")
            
            # Download image
            temp_image = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False).name
            await download_video_from_url(image_url, temp_image)
            
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
        else:
            logging.info(f"[process_clip] No reference found for clip {idx}, using original")
            await redis_client.set(f"job:{job_id}:clip:{idx}:has_replacement", "false")
        
        # Atomic increment and check if all done
        done = await redis_client.incr(f"job:{job_id}:done")
        total = int(await redis_client.get(f"job:{job_id}:total") or 0)
        
        logging.info(f"[process_clip] Job {job_id}: {done}/{total} clips done")
        
        if done == total:
            await ctx['pool'].enqueue_job('stitch_video', job_id)
            logging.info(f"[process_clip] All clips done, enqueuing stitch_video")
    
    except Exception as e:
        logging.error(f"[process_clip] Job {job_id}, clip {idx} failed: {e}")
        raise
    
    finally:
        if redis_client:
            await redis_client.close()
        
        for temp_file in [temp_clip, temp_audio, temp_image, temp_replacement]:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)

async def mock_transcribe(video_path: str) -> str:
    """
    Mock transcription function.
    Replace with Deepgram or ElevenLabs API call.
    """
    await asyncio.sleep(0.5)
    return "This is a mock transcript mentioning Steve Jobs and the iPhone launch in 2007."

async def mock_perplexity_check(transcript: str) -> dict:
    """
    Mock Perplexity API call.
    Replace with actual Perplexity API integration.
    
    Returns:
        dict with 'has_reference' (bool) and 'image_url' (str) if reference found
    """
    await asyncio.sleep(0.5)
    
    keywords = ['steve jobs', 'iphone', 'apple', 'launch']
    has_reference = any(keyword in transcript.lower() for keyword in keywords)
    
    if has_reference:
        return {
            'has_reference': True,
            'image_url': 'https://iyvkdwkdzcridtiioofc.supabase.co/storage/v1/object/public/perplexed/richard%20feynman.png'
        }
    
    return {'has_reference': False}

async def stitch_video(ctx, job_id: str):
    logging.info(f"[stitch_video] Starting job {job_id}")
    
    redis_client = await get_redis_client()
    
    await asyncio.sleep(2)
    
    await redis_client.set(f"job:{job_id}:status", "finished")
    logging.info(f"[stitch_video] Job {job_id} finished!")
    
    await redis_client.close()

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
    max_jobs = 3