import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
if not os.getenv('DOCKER_CONTAINER'):
    load_dotenv(os.path.join(basedir, os.getenv('ENV_FILE', '.env.local')), override=True)

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY') or 'you-will-never-guess'
    QUART_HOST = os.getenv('QUART_HOST', '0.0.0.0')
    QUART_DEBUG = os.getenv('QUART_DEBUG', '0')
    QUART_PORT = os.getenv('QUART_PORT', '8080')
    FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:3000')

    # Supabase config
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_SECRET_KEY')
    SUPABASE_BUCKET = os.getenv('SUPABASE_BUCKET', 'uploads')

    # OpenAI config
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

    # Download directory
    DOWNLOAD_DIR = os.path.join(basedir, os.getenv('DOWNLOAD_DIR', 'runs'))
    REMOTE_DIR = os.getenv('DOWNLOAD_DIR', 'runs')

    # Redis config
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '0'))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    REDIS_SSL = os.getenv('REDIS_SSL', 'false').lower() == 'true'