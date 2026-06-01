import base64

from openai_rq import codec


def test_stream_field_roundtrip_head():
    entry = codec.stream_head(status=200, headers={"content-type": "text/event-stream"})
    fields = codec.encode_stream_field(entry)
    assert set(fields) == {"data"}
    parsed = codec.parse_stream_field(fields)
    assert parsed["t"] == "head"
    assert parsed["status"] == 200
    assert parsed["headers"]["content-type"] == "text/event-stream"


def test_stream_data_roundtrip():
    entry = codec.stream_data(b'data: {"x":1}\n\n')
    parsed = codec.parse_stream_field(codec.encode_stream_field(entry))
    assert parsed["t"] == "data"
    assert codec.stream_data_bytes(parsed) == b'data: {"x":1}\n\n'


def test_stream_done_and_error():
    done = codec.parse_stream_field(codec.encode_stream_field(codec.stream_done()))
    assert done["t"] == "done"

    err = codec.stream_error(status=502, headers={}, body=b"boom")
    perr = codec.parse_stream_field(codec.encode_stream_field(err))
    assert perr["t"] == "error"
    assert perr["status"] == 502
    assert base64.b64decode(perr["b"]) == b"boom"


def test_parse_handles_bytes_field_keys():
    # redis-py returns bytes keys/values by default; parser must cope.
    entry = codec.stream_data(b"abc")
    fields = codec.encode_stream_field(entry)
    byteish = {k.encode(): v.encode() for k, v in fields.items()}
    parsed = codec.parse_stream_field(byteish)
    assert codec.stream_data_bytes(parsed) == b"abc"
