import socket
from typing import List
from app.parser import RESPSerializer

def send_command(cmd: List, host="localhost", port=6379) -> bytes:
    """
    Send a command (text, not RESP) to the server and return raw RESP reply.
    """
    with socket.create_connection((host, port)) as sock:
        cmd_bytes = RESPSerializer.serialize_array(cmd)
        sock.sendall(cmd_bytes)
        return sock.recv(4096)


def assert_command(cmd: List, expected: bytes):
    print(f"\n[tester] [client] > {cmd}")
    resp = send_command(cmd)
    
    if not expected:
        return
    
    print(f"\n[tester] Expected: {expected} | Received: {resp}")
    assert resp == expected, f"{cmd} -> Expected: {expected} | Received: {resp}"