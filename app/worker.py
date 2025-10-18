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
    logging.info(f"[process_clip] Processing job {job_id}, clip {idx}")
    
    redis_client = await get_redis_client()
    
    await asyncio.sleep(1)
    
    done = await redis_client.incr(f"job:{job_id}:done")
    total = int(await redis_client.get(f"job:{job_id}:total") or 0)
    
    logging.info(f"[process_clip] Job {job_id}: {done}/{total} clips done")
    
    if done == total:
        await ctx['pool'].enqueue_job('stitch_video', job_id)
        logging.info(f"[process_clip] All clips done, enqueuing stitch_video")
    
    await redis_client.close()

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
    max_jobs = 10