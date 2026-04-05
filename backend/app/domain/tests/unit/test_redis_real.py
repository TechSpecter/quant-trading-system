import redis


def test_redis_connection():
    client = redis.Redis(host="localhost", port=6379, db=0)

    # SET
    client.set("test_key", "hello")

    # GET
    value = client.get("test_key")

    assert value is not None
    decoded_value = (
        value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
    )
    assert decoded_value == "hello"

    print("✅ Redis working")
