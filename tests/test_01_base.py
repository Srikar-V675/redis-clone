import socket
import time
import pytest
from tests.helpers import assert_command
from app.parser import RESPSerializer

@pytest.mark.parametrize("cmd,expected", [
    (["PING"], RESPSerializer.serialize_simple_string("PONG")),
    (["PiNg"], RESPSerializer.serialize_simple_string("PONG")),  # case-insensitivity check (optional)
])
def test_ping(server, cmd, expected):
    print("\n[tester] Testing #S1: Respond to PING")
    assert_command(cmd, expected)


def test_multiple_pings(server):
    print("\n[tester] Testing #S2: Respond to Multiple PINGs")
    for _ in range(3):
        assert_command(["PING"], RESPSerializer.serialize_simple_string("PONG"))


def test_concurrent_clients(server):
    print("\n[tester] Testing #S3: Handle Concurrent Clients")
    s1 = socket.create_connection(("localhost", 6379))
    s2 = socket.create_connection(("localhost", 6379))

    cmd = ["PING"]
    expected = RESPSerializer.serialize_simple_string("PONG")

    # client 1
    s1.sendall(RESPSerializer.serialize_array(cmd))
    resp1 = s1.recv(1024)
    print("[tester] [client-1] > PING")
    print(f"[tester] Expected: {expected} | Received: {resp1}")
    assert resp1 == expected

    # client 2
    s2.sendall(RESPSerializer.serialize_array(cmd))
    resp2 = s2.recv(1024)
    print("[tester] [client-2] > PING")
    print(f"[tester] Expected: {expected} | Received: {resp2}")
    assert resp2 == expected

    s1.close()
    s2.close()


def test_echo(server):
    print("\n[tester] Testing #S4: Implement ECHO Command")
    assert_command(
        ["ECHO", "hello"],
        RESPSerializer.serialize_bulk_string("hello")
    )


def test_set_get(server):
    print("\n[tester] Testing #S5: Implement SET & GET Commands")
    assert_command(
        ["SET", "set_cmd", "bar"],
        RESPSerializer.serialize_simple_string("OK")
    )
    assert_command(
        ["GET", "set_cmd"],
        RESPSerializer.serialize_bulk_string("bar")
    )


def test_expiry(server):
    print("\n[tester] Testing #S6: Expiry")
    assert_command(
        ["SET", "set_expiry", "bar", "px", "100"],
        RESPSerializer.serialize_simple_string("OK")
    )
    time.sleep(0.2)
    assert_command(
        ["GET", "set_expiry"],
        b"_\r\n"  # Null bulk string in RESP3
    )


def test_type(server):
    print("\n[tester] Testing #S17: TYPE Command")
    assert_command(
        ["SET", "ktype", "foo"],
        RESPSerializer.serialize_simple_string("OK")
    )
    assert_command(
        ["TYPE", "ktype"],
        RESPSerializer.serialize_simple_string("string")
    )
    assert_command(
        ["TYPE", "blah"],
        RESPSerializer.serialize_simple_string("none")
    )