import pytest_asyncio
from app import create_app

class TestConfig:
    TESTING = True
    SECRET_KEY = 'test-key'
    PROPAGATE_EXCEPTIONS = True
    FRONTEND_URL = 'http://localhost:3000'
    
    # Dummy SUPABASE values
    SUPABASE_URL = 'https://test.supabase.co'
    SUPABASE_KEY = 'test-key'
    SUPABASE_BUCKET = 'test-bucket'

    # Dummy OpenAI value
    OPENAI_API_KEY = 'test-openai-key'

@pytest_asyncio.fixture
async def app():
    """ Create and configure a new Quart app instance for tests. """
    app = create_app(TestConfig)
    yield app

@pytest_asyncio.fixture
async def client(app):
    """ Async Quart test client fixture. """
    yield app.test_client()
