import socket
import time
import pytest
from tests.helpers import assert_command
from app.parser import RESPSerializer


def test_create_list(server):
    print("\n[tester] Testing #S7: Create a List")
    assert_command(
        ["RPUSH", "foo", "bar"],
        RESPSerializer.serialize_integer(1)
    )


def test_append_element(server):
    print("\n[tester] Testing #S8: Append an Element")
    assert_command(
        ["RPUSH", "foo", "beer"],
        RESPSerializer.serialize_integer(2)
    )


def test_append_multiple_elements(server):
    print("\n[tester] Testing #S9: Append Multiple Elements")
    assert_command(
        ["RPUSH", "foo", "bear", "man", "animal"],
        RESPSerializer.serialize_integer(5)
    )


@pytest.mark.parametrize("cmd,expected", [
    (["LRANGE", "foo", "0", "1"], RESPSerializer.serialize_array(["bar", "beer"])),
    (["LRANGE", "foo", "2", "4"], RESPSerializer.serialize_array(["bear", "man", "animal"])),
    (["LRANGE", "foo", "5", "6"], RESPSerializer.serialize_array([])),
    (["LRANGE", "foo", "2", "6"], RESPSerializer.serialize_array(["bear", "man", "animal"])),
    (["LRANGE", "foo", "4", "2"], RESPSerializer.serialize_array([])),
    (["LRANGE", "bar", "1", "3"], RESPSerializer.serialize_array([])),
])
def test_list_elements_positive_idx(server, cmd, expected):
    print("\n[tester] Testing #S10: List Elements (Positive Index)")
    assert_command(cmd, expected)


@pytest.mark.parametrize("cmd,expected", [
    (["LRANGE", "foo", "-2", "-1"], RESPSerializer.serialize_array(["man", "animal"])),
    (["LRANGE", "foo", "0", "-4"], RESPSerializer.serialize_array(["bar", "beer"])),
    (["LRANGE", "foo", "0", "-6"], RESPSerializer.serialize_array([])),
    (["LRANGE", "foo", "-7", "-4"], RESPSerializer.serialize_array(["bar", "beer"])),
    (["LRANGE", "foo", "-7", "-6"], RESPSerializer.serialize_array([])),
])
def test_list_elements_negative_idx(server, cmd, expected):
    print("\n[tester] Testing #S11: List Elements (Negative Index)")
    assert_command(cmd, expected)


def test_prepend_elements(server):
    print("\n[tester] Testing #S12: Prepend Elements")
    assert_command(
        ["LPUSH", "lfoo", "c"],
        RESPSerializer.serialize_integer(1)
    )
    assert_command(
        ["LPUSH", "lfoo", "b", "a"],
        RESPSerializer.serialize_integer(3)
    )
    assert_command(
        ["LRANGE", "lfoo", "0", "-1"],
        RESPSerializer.serialize_array(["a", "b", "c"])
    )


def test_list_length(server):
    print("\n[tester] Testing #S13: Query List Length")
    assert_command(["LLEN", "foo"], RESPSerializer.serialize_integer(5))
    assert_command(["LLEN", "lfoo"], RESPSerializer.serialize_integer(3))
    assert_command(["LLEN", "fool"], RESPSerializer.serialize_integer(0))


def test_remove_element(server):
    print("\n[tester] Testing #S14: Remove an Element")
    assert_command(
        ["LPOP", "lfoo"],
        RESPSerializer.serialize_bulk_string("a")
    )
    assert_command(
        ["LRANGE", "lfoo", "0", "-1"],
        RESPSerializer.serialize_array(["b", "c"])
    )
    assert_command(
        ["LPOP", "fool"],
        b"_\r\n"   # null bulk string
    )


def test_remove_multiple_elements(server):
    print("\n[tester] Testing #S14: Remove Multiple Elements")
    assert_command(
        ["LPOP", "lfoo", "2"],
        RESPSerializer.serialize_array(["b", "c"])
    )
    assert_command(
        ["LRANGE", "lfoo", "0", "-1"],
        RESPSerializer.serialize_array([])
    )
    assert_command(
        ["LPOP", "foo", "6"],
        RESPSerializer.serialize_array(["bar", "beer", "bear", "man", "animal"])
    )


def test_blocking_retrieval(server):
    print("\n[tester] Testing #S15: Blocking Retrieval")

    # Non-blocking case
    assert_command(
        ["RPUSH", "blfoo", "a"],
        RESPSerializer.serialize_integer(1)
    )
    assert_command(
        ["BLPOP", "blfoo", "0"],
        RESPSerializer.serialize_array(["blfoo", "a"])
    )

    # One blocking client
    s1 = socket.create_connection(("localhost", 6379))
    s1.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo1", "0"]))
    time.sleep(1)
    assert_command(["RPUSH", "blfoo1", "a"], RESPSerializer.serialize_integer(0))
    resp1 = s1.recv(1024)
    assert resp1 == RESPSerializer.serialize_array(["blfoo1", "a"])
    s1.close()

    # Two blocking clients
    s1 = socket.create_connection(("localhost", 6379))
    s2 = socket.create_connection(("localhost", 6379))
    s1.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo", "0"]))
    time.sleep(1)
    s2.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo", "0"]))
    time.sleep(1)

    assert_command(["RPUSH", "blfoo", "a"], RESPSerializer.serialize_integer(0))
    resp1 = s1.recv(1024)
    assert resp1 == RESPSerializer.serialize_array(["blfoo", "a"])

    time.sleep(1)
    assert_command(["RPUSH", "blfoo", "b"], RESPSerializer.serialize_integer(0))
    resp2 = s2.recv(1024)
    assert resp2 == RESPSerializer.serialize_array(["blfoo", "b"])

    # Two blocking + >2 values pushed
    s1.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo", "0"]))
    time.sleep(1)
    s2.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo", "0"]))
    time.sleep(1)

    assert_command(["RPUSH", "blfoo", "a", "b", "c"], RESPSerializer.serialize_integer(1))
    resp1 = s1.recv(1024)
    assert resp1 == RESPSerializer.serialize_array(["blfoo", "a"])
    resp2 = s2.recv(1024)
    assert resp2 == RESPSerializer.serialize_array(["blfoo", "b"])

    assert_command(
        ["LRANGE", "blfoo", "0", "-1"],
        RESPSerializer.serialize_array(["c"])
    )

    s1.close()
    s2.close()


def test_blocking_retrieval_with_timeout(server):
    print("\n[tester] Testing #S16: Blocking Retrieval with Timeout")

    s1 = socket.create_connection(("localhost", 6379))
    s1.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo1", "1"]))
    time.sleep(1.1)
    resp1 = s1.recv(1024)
    assert resp1 == b"_\r\n"  # null when timeout expires

    s1.sendall(RESPSerializer.serialize_array(["BLPOP", "blfoo1", "1"]))
    time.sleep(0.5)
    assert_command(["RPUSH", "blfoo1", "a"], RESPSerializer.serialize_integer(0))
    resp1 = s1.recv(1024)
    assert resp1 == RESPSerializer.serialize_array(["blfoo1", "a"])
    s1.close()
