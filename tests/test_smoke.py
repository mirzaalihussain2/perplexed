import pytest

@pytest.mark.asyncio
async def test_health(client):
    """ Test basic healthcheck endpoint. """
    response = await client.get("/health")

    assert response.status_code == 200
    data = await response.get_json()
    assert data["data"]["message"] == "Hello, World!"