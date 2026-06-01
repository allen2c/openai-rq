import json
import threading

from openai_rq import AsyncOpenAIRQ, OpenAIRQ, codec


def _fake_chat_worker(redis):
    while True:
        entries = redis.xread({codec.REQUESTS_STREAM: "0"}, count=1, block=3000)
        if entries:
            break
    job = json.loads(entries[0][1][0][1][b"data"])
    body = json.dumps(
        {
            "id": "cmpl-1",
            "object": "chat.completion",
            "created": 0,
            "model": "x",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "hi"},
                    "finish_reason": "stop",
                }
            ],
        }
    ).encode()
    payload = codec.encode_response(
        status=200, headers={"content-type": "application/json"}, body=body
    )
    redis.rpush(codec.result_key(job["id"]), json.dumps(payload))


def test_openai_rq_chat_completion(redis_client):
    client = OpenAIRQ(redis_client=redis_client)
    t = threading.Thread(target=_fake_chat_worker, args=(redis_client,))
    t.start()
    completion = client.chat.completions.create(
        model="x", messages=[{"role": "user", "content": "hello"}]
    )
    t.join(timeout=5)
    assert completion.choices[0].message.content == "hi"


async def test_clients_construct_with_defaults(redis_client, async_redis_client):
    assert OpenAIRQ(redis_client=redis_client).max_retries == 0
    assert AsyncOpenAIRQ(redis_client=async_redis_client).max_retries == 0
