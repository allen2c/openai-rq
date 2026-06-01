import base64

from openai_rq import codec


def test_encode_decode_response_roundtrip():
    payload = codec.encode_response(
        status=200,
        headers={"content-type": "application/json", "content-length": "5"},
        body=b"hello",
    )
    # content-length is stripped (client httpx recomputes it)
    assert "content-length" not in {k.lower() for k in payload["headers"]}
    assert payload["status"] == 200
    assert base64.b64decode(payload["body_b64"]) == b"hello"

    status, headers, body = codec.decode_response(payload)
    assert status == 200
    assert headers["content-type"] == "application/json"
    assert body == b"hello"


def test_encode_response_serializable():
    import json

    payload = codec.encode_response(status=404, headers={}, body=b'{"error":1}')
    # must survive a JSON round-trip (it travels through Redis as a string)
    again = json.loads(json.dumps(payload))
    status, _, body = codec.decode_response(again)
    assert status == 404
    assert body == b'{"error":1}'
