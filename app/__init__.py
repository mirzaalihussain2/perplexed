from quart import Quart, jsonify
from quart_cors import cors
from config import Config
from app.common.types import ApiResponse
from http import HTTPStatus
from supabase import create_client, Client
from openai import AsyncOpenAI
import redis.asyncio as redis
import logging

# Removes "ERROR:root:" from the start of each error log
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO
)

def create_app(config_class=Config):
    app = Quart(__name__)
    app.config.from_object(config_class)

    app = cors(app,
        allow_origin=app.config['FRONTEND_URL'],
        allow_methods=['GET', 'POST'],
        allow_headers=['Content-Type']
    )

    if not app.config['SUPABASE_URL'] or not app.config['SUPABASE_KEY']:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY is missing")

    # Initialize Supabase client once at startup
    app.supabase = create_client(
        app.config['SUPABASE_URL'],
        app.config['SUPABASE_KEY']
    )

    # Initialise Redis client
    app.redis = redis.Redis(
        host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'],
        password=app.config['REDIS_PASSWORD'],
        ssl=app.config['REDIS_SSL'],
        ssl_cert_reqs=None,  # Add this for Upstash
        decode_responses=True
    )

    # # Initialise OpenAI client once at startup
    # app.openai = AsyncOpenAI(api_key=app.config['OPENAI_API_KEY'])

    # Register blueprints
    from app.routes import bp as bp
    app.register_blueprint(bp, url_prefix="/")

    @app.route('/health', methods=['GET'])
    async def api_endpoint():
        response = ApiResponse(
            success=True,
            data={ "message": "Hello, World!" }
        )

        return jsonify(response.model_dump()), HTTPStatus.OK
    
    return app