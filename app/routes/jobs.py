from quart import Blueprint, jsonify, request, current_app
from app.common.types import ApiResponse, ErrorDetail
from app.routes import bp
from http import HTTPStatus
from arq import create_pool
from arq.connections import RedisSettings
import uuid

@bp.route('/jobs', methods=['POST'])
async def create_job():
    try:
        data = await request.get_json()
        
        if not data or 'video_path' not in data:
            response = ApiResponse(
                success=False,
                error=ErrorDetail(
                    code="INVALID_REQUEST",
                    message="video_path is required"
                )
            )
            return jsonify(response.model_dump()), HTTPStatus.BAD_REQUEST
        
        video_path = data['video_path']
        job_id = str(uuid.uuid4())
        
        redis_client = current_app.redis
        await redis_client.set(f"job:{job_id}:status", "queued", ex=172800)
        await redis_client.set(f"job:{job_id}:video_path", video_path, ex=172800)
        await redis_client.set(f"job:{job_id}:total", "0", ex=172800)
        await redis_client.set(f"job:{job_id}:done", "0", ex=172800)
        
        redis_settings = RedisSettings(
            host=current_app.config['REDIS_HOST'],
            port=current_app.config['REDIS_PORT'],
            database=current_app.config['REDIS_DB'],
            password=current_app.config['REDIS_PASSWORD'],
            ssl=current_app.config['REDIS_SSL'],
            ssl_cert_reqs=None
        )
        pool = await create_pool(redis_settings)
        
        await pool.enqueue_job('split_video', job_id)
        
        response = ApiResponse(
            success=True,
            data={
                "job_id": job_id,
                "status": "queued",
                "status_url": f"/jobs/{job_id}"
            }
        )
        return jsonify(response.model_dump()), HTTPStatus.CREATED
        
    except Exception as e:
        current_app.logger.error(f"Error creating job: {str(e)}")
        response = ApiResponse(
            success=False,
            error=ErrorDetail(
                code="INTERNAL_ERROR",
                message="Failed to create job"
            )
        )
        return jsonify(response.model_dump()), HTTPStatus.INTERNAL_SERVER_ERROR


@bp.route('/jobs/<job_id>', methods=['GET'])
async def get_job_status(job_id):
    redis = current_app.redis
    
    status = await redis.get(f"job:{job_id}:status")
    if not status:
        return jsonify(ApiResponse(
            success=False,
            message="Job not found",
            error=ErrorDetail(code="NOT_FOUND", message="Job not found")
        ).model_dump()), HTTPStatus.NOT_FOUND
    
    total = await redis.get(f"job:{job_id}:total") or "0"
    done = await redis.get(f"job:{job_id}:done") or "0"
    error = await redis.get(f"job:{job_id}:error")
    final_url = await redis.get(f"job:{job_id}:final_url")
    
    response_data = {
        "job_id": job_id,
        "status": status,
        "total": int(total),
        "done": int(done)
    }
    
    if error:
        response_data["error"] = error
    
    if final_url:
        response_data["final_url"] = final_url
    
    return jsonify(ApiResponse(
        success=True,
        message="Job status retrieved",
        data=response_data
    ).model_dump()), HTTPStatus.OK