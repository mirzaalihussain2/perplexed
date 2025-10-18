import asyncio
import logging
from arq import create_pool
from arq.connections import RedisSettings
from config import Config
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO, format='%(message)s')

async def get_redis_client():
    return redis.Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        db=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        ssl=Config.REDIS_SSL,
        ssl_cert_reqs=None,  # Add this
        decode_responses=True
    )

async def split_video(ctx, job_id: str):
    logging.info(f"[split_video] Starting job {job_id}")
    
    redis_client = await get_redis_client()
    await redis_client.set(f"job:{job_id}:status", "processing")
    
    video_path = await redis_client.get(f"job:{job_id}:video_path")
    logging.info(f"[split_video] Video path: {video_path}")
    
    await asyncio.sleep(2)
    
    num_chunks = 3
    await redis_client.set(f"job:{job_id}:total", str(num_chunks))
    
    for idx in range(num_chunks):
        await ctx['pool'].enqueue_job('process_clip', job_id, idx)
    
    logging.info(f"[split_video] Enqueued {num_chunks} clip tasks")
    await redis_client.close()

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
        ssl_cert_reqs=None  # Add this
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
        ssl_cert_reqs=None  # Add this
    )
    functions = [split_video, process_clip, stitch_video]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10