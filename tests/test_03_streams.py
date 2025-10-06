from tests.helpers import assert_command
from app.parser import RESPSerializer
import pytest
import time
from unittest.mock import patch
from app.commands.stream import XAddCommand

def test_xadd(server):
    print("\n[tester] Testing XADD Command")
    assert_command(
        ["XADD", "stream_key", "0-1", "foo", "bar"],
        RESPSerializer.serialize_bulk_string("0-1")
    )


@pytest.mark.parametrize( "cmd,expected", [
    (["XADD", "stream_key", "1-1", "foo", "bar"], RESPSerializer.serialize_bulk_string("1-1")),
    (["XADD", "stream_key", "1-1", "foo", "bar"], RESPSerializer.serialize_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    (["XADD", "stream_key", "0-3", "foo", "bar"], RESPSerializer.serialize_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    (["XADD", "stream_key_empty", "0-0", "foo", "bar"], RESPSerializer.serialize_error("ERR The ID specified in XADD must be greater than 0-0"))
])
def test_xadd_entry_ids(server, cmd, expected):
    print("\n[tester] Testing XADD entry IDs")
    assert_command(cmd, expected)


@pytest.mark.parametrize("cmd,expected", [
    (["XADD", "stream_gen", "0-*", "foo", "bar"], RESPSerializer.serialize_bulk_string("0-1")),
    (["XADD", "stream_gen", "5-*", "foo", "bar"], RESPSerializer.serialize_bulk_string("5-0")),
    (["XADD", "stream_gen", "5-*", "foo", "bar"], RESPSerializer.serialize_bulk_string("5-1")),
    (["XADD", "stream_gen", "4-*", "foo", "bar"], RESPSerializer.serialize_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    (["XADD", "stream_gen", "0-*", "foo", "bar"], RESPSerializer.serialize_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    (["XADD", "stream_gen_empty", "5-*", "foo", "bar"], RESPSerializer.serialize_bulk_string("5-0"))
])
def test_xadd_seq_generate(server, cmd, expected):
    print("\n[tester] Testing XADD generate seq ID")
    assert_command(cmd, expected)


@pytest.mark.parametrize("cmd,expected", [
    # Empty stream
    (["XRANGE", "stream_empty", "-", "+"], RESPSerializer.serialize_array([])),

    # Single entry stream
    (["XADD", "stream_single", "0-1", "foo", "bar"], RESPSerializer.serialize_bulk_string("0-1")),
    (["XRANGE", "stream_single", "-", "+"], RESPSerializer.serialize_array([["0-1", ["foo", "bar"]]])),
    (["XRANGE", "stream_single", "0-1", "0-1"], RESPSerializer.serialize_array([["0-1", ["foo", "bar"]]])),
    (["XRANGE", "stream_single", "0-1", "0-2"], RESPSerializer.serialize_array([["0-1", ["foo", "bar"]]])),

    # Multiple entries
    (["XADD", "stream_multi", "1-0", "a", "1"], RESPSerializer.serialize_bulk_string("1-0")),
    (["XADD", "stream_multi", "2-0", "b", "2"], RESPSerializer.serialize_bulk_string("2-0")),
    (["XADD", "stream_multi", "3-0", "c", "3"], RESPSerializer.serialize_bulk_string("3-0")),

    # XRANGE full range
    (["XRANGE", "stream_multi", "-", "+"], RESPSerializer.serialize_array([
        ["1-0", ["a", "1"]],
        ["2-0", ["b", "2"]],
        ["3-0", ["c", "3"]]
    ])),

    # XRANGE with start only (min seq)
    (["XRANGE", "stream_multi", "2", "+"], RESPSerializer.serialize_array([
        ["2-0", ["b", "2"]],
        ["3-0", ["c", "3"]]
    ])),

    # XRANGE with stop only (max seq)
    (["XRANGE", "stream_multi", "-", "2"], RESPSerializer.serialize_array([
        ["1-0", ["a", "1"]],
        ["2-0", ["b", "2"]]
    ])),

    # XRANGE with start > stop should return empty
    (["XRANGE", "stream_multi", "3-0", "1-0"], RESPSerializer.serialize_array([])),

    # XRANGE with exact ID match
    (["XRANGE", "stream_multi", "2-0", "2-0"], RESPSerializer.serialize_array([
        ["2-0", ["b", "2"]]
    ]))
])
def test_xrange(server, cmd, expected):
    print("\n[tester] Testing XRANGE variations")
    assert_command(cmd, expected)


@pytest.mark.parametrize("setup_commands, xread_cmd, expected", [
    # Single stream, normal read
    (
        [["XADD", "stream_a1", "0-1", "foo", "bar"]],
        ["XREAD", "STREAMS", "stream_a1", "0-0"],
        RESPSerializer.serialize_array([["stream_a1", [["0-1", ["foo", "bar"]]]]])
    ),
    # Multiple streams, some empty
    (
        [["XADD", "stream_b1", "0-1", "foo", "bar"]],
        ["XREAD", "STREAMS", "stream_b1", "stream_b2", "0-0", "0-0"],
        RESPSerializer.serialize_array([
            ["stream_b1", [["0-1", ["foo", "bar"]]]],
            ["stream_b2", []]
        ])
    ),
    # Non-existent stream
    (
        [],
        ["XREAD", "STREAMS", "stream_c1", "0-0"],
        RESPSerializer.serialize_array([["stream_c1", []]])
    ),
    # Start ID greater than entry -> should skip
    (
        [["XADD", "stream_d1", "0-1", "foo", "bar"]],
        ["XREAD", "STREAMS", "stream_d1", "1-0"],
        RESPSerializer.serialize_array([["stream_d1", []]])
    ),
    # Start ID using "-" (smallest) -> includes all
    (
        [["XADD", "stream_e1", "0-1", "foo", "bar"]],
        ["XREAD", "STREAMS", "stream_e1", "-"],
        RESPSerializer.serialize_array([["stream_e1", [["0-1", ["foo", "bar"]]]]])
    ),
    # Start ID using "+" (largest) -> nothing to return
    (
        [["XADD", "stream_f1", "0-1", "foo", "bar"]],
        ["XREAD", "STREAMS", "stream_f1", "+"],
        RESPSerializer.serialize_array([["stream_f1", []]])
    )
])
def test_xread_streams(server, setup_commands, xread_cmd, expected):
    # Setup initial state
    for cmd in setup_commands:
        assert_command(cmd, None)  # We don't care about XADD return here
    
    print("\n[tester] Testing XREAD STREAMS command")
    assert_command(xread_cmd, expected)
