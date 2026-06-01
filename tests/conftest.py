"""Shared test fixtures.

Tests run against a REAL Redis (closest-to-production behaviour), isolated to a
throwaway DB and flushed before/after each test. Override the target with
OPENAI_RQ_TEST_REDIS_URL (default: redis://127.0.0.1:6379/15).
"""

import os

import pytest
import pytest_asyncio
import redis
import redis.asyncio as aioredis

REDIS_URL = os.environ.get("OPENAI_RQ_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


@pytest.fixture
def redis_client():
    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()


@pytest_asyncio.fixture
async def async_redis_client():
    client = aioredis.Redis.from_url(REDIS_URL)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
