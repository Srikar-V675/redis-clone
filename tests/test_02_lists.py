import socket
import time
from helpers import send_command, to_resp_array, parse_resp


def test_01_create_list(server):
    print("\n[tester] Testing #S7: Create a List")
    resp = send_command("RPUSH foo bar")
    print("[tester] [client] > RPUSH foo bar")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 1

def test_02_append_element(server):
    print("\n[tester] Testing #S8: Append an Element")
    resp = send_command("RPUSH foo beer")
    print("[tester] [client] > RPUSH foo beer")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 2

def test_03_append_multiple_element(server):
    print("\n[tester] Testing #S9: Append Multiple Elements")
    resp = send_command("RPUSH foo bear man animal")
    print("[tester] [client] > RPUSH foo bear man animal")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 5

def test_04_list_elements_positive_idx(server):
    print("\n[tester] Testing #S10: List Elements(Positive Index)")
    
    # within range
    resp = send_command("LRANGE foo 0 1")
    print("[tester] [client] > LRANGE foo 0 1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bar", "beer"]
    
    resp = send_command("LRANGE foo 2 4")
    print("[tester] [client] > LRANGE foo 2 4")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bear", "man", "animal"]
    
    # start index >= length
    resp = send_command("LRANGE foo 5 6")
    print("[tester] [client] > LRANGE foo 5 6")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []
    
    # stop index >= length
    resp = send_command("LRANGE foo 2 6")
    print("[tester] [client] > LRANGE foo 2 6")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bear", "man", "animal"]
    
    # stop > start
    resp = send_command("LRANGE foo 4 2")
    print("[tester] [client] > LRANGE foo 4 2")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []
    
    # does not exist
    resp = send_command("LRANGE bar 1 3")
    print("[tester] [client] > LRANGE bar 1 3")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []


def test_05_list_elements_negative_idx(server):
    print("\n[tester] Testing #S11: List Elements(Negative Index)")
    
    # within range
    resp = send_command("LRANGE foo -2 -1")
    print("[tester] [client] > LRANGE foo -2 -1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["man", "animal"]
    
    resp = send_command("LRANGE foo 0 -4")
    print("[tester] [client] > LRANGE foo 0 -4")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bar", "beer"]
    
    # out of range -> TODO: check 
    resp = send_command("LRANGE foo 0 -6")
    print("[tester] [client] > LRANGE foo 0 -6")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []
    
    resp = send_command("LRANGE foo -7 -4")
    print("[tester] [client] > LRANGE foo -7 -4")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bar", "beer"]
    
    resp = send_command("LRANGE foo -7 -6")
    print("[tester] [client] > LRANGE foo -7 -6")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []

def test_06_prepend_elements(server):
    print("\n[tester] Testing #S12: Prepend Elements")
    resp = send_command("LPUSH lfoo c")
    print("[tester] [client] > LPUSH lfoo c")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 1
    
    resp = send_command("LPUSH lfoo b a")
    print("[tester] [client] > LPUSH lfoo b a")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 3
    
    resp = send_command("LRANGE lfoo 0 -1")
    print("[tester] [client] > LRANGE lfoo 0 -1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["a", "b", "c"]

def test_07_list_length(server):
    print("\n[tester] Testing #S13: Query List Length")
    resp = send_command("LLEN foo")
    print("[tester] [client] > LLEN foo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 5
    
    resp = send_command("LLEN lfoo")
    print("[tester] [client] > LLEN lfoo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 3
    
    resp = send_command("LLEN fool")
    print("[tester] [client] > LLEN fool")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == 0

def test_08_remove_element(server):
    print("\n[tester] Testing #S14: REmove an Element")
    resp = send_command("LPOP lfoo")
    print("[tester] [client] > LPOP lfoo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "a"
    
    resp = send_command("LRANGE lfoo 0 -1")
    print("[tester] [client] > LRANGE lfoo 0 -1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["b", "c"]
    
    resp = send_command("LPOP fool")
    print("[tester] [client] > LPOP fool")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == None

def test_09_remove_multiple_elements(server):
    print("\n[tester] Testing #S14: Remove Multiple Elements")
    resp = send_command("LPOP lfoo 2")
    print("[tester] [client] > LPOP lfoo 2")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["b", "c"]
    
    resp = send_command("LRANGE lfoo 0 -1")
    print("[tester] [client] > LRANGE lfoo 0 -1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == []
    
    resp = send_command("LPOP foo 6")
    print("[tester] [client] > LPOP foo 6")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["bar", "beer", "bear", "man", "animal"]

def test_10_blocking_retrieval(server):
    print("\n[tester] Testing #S15: Blocking Retrieval")
    resp = send_command("RPUSH blfoo a")
    print("[tester] [client] > RPUSH blfoo a")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    
    resp = send_command("BLPOP blfoo 0")
    print("\n[tester] [client] > BLPOP blfoo 0")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["blfoo", "a"]
    
    # One blocking client 
    s1 = socket.create_connection(("localhost", 6379))
    
    print("[tester] [client-1] > BLPOP blfoo1 0")
    print("[tester] Waiting...")
    s1.sendall(to_resp_array("BLPOP blfoo1 0"))
    
    time.sleep(1)
    resp = send_command("RPUSH blfoo1 a")
    print("[tester] [client-2] > RPUSH blfoo1 a")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    
    resp1 = s1.recv(1024)
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == ["blfoo1", "a"]
    
    # Two blocking clients
    s2 = socket.create_connection(("localhost", 6379))
    
    print("[tester] [client-1] > BLPOP blfoo 0")
    print("[tester] Waiting...")
    s1.sendall(to_resp_array("BLPOP blfoo 0"))
    
    time.sleep(1)
    print("[tester] [client-2] > BLPOP blfoo 0")
    print("[tester] Waiting...")
    s2.sendall(to_resp_array("BLPOP blfoo 0"))
    
    time.sleep(1)
    resp = send_command("RPUSH blfoo a")
    print("[tester] [client-3] > RPUSH blfoo a")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    
    resp1 = s1.recv(1024)
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == ["blfoo", "a"]
    
    time.sleep(1)
    resp = send_command("RPUSH blfoo b")
    print("[tester] [client-3] > RPUSH blfoo b")
    parsed = parse_resp(resp)
    print(f"[tester] [client-3] Received {parsed}")
    
    resp2 = s2.recv(1024)
    parsed2 = parse_resp(resp2)
    print(f"[tester] [client-2] Received {parsed2}")
    assert parsed2 == ["blfoo", "b"]
    
    # Two blocking client + >2 values added at once
    print("[tester] [client-1] > BLPOP blfoo 0")
    print("[tester] Waiting...")
    s1.sendall(to_resp_array("BLPOP blfoo 0"))
    
    time.sleep(1)
    print("[tester] [client-2] > BLPOP blfoo 0")
    print("[tester] Waiting...")
    s2.sendall(to_resp_array("BLPOP blfoo 0"))
    
    time.sleep(1)
    resp = send_command("RPUSH blfoo a b c")
    print("[tester] [client-3] > RPUSH blfoo a b c")
    parsed = parse_resp(resp)
    print(f"[tester] [client-3] Received {parsed}")
    
    resp1 = s1.recv(1024)
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == ["blfoo", "a"]
    
    resp2 = s2.recv(1024)
    parsed2 = parse_resp(resp2)
    print(f"[tester] [client-2] Received {parsed2}")
    assert parsed2 == ["blfoo", "b"]
    
    resp = send_command("LRANGE blfoo 0 -1")
    print("[tester] [client] > LRANGE blfoo 0 -1")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == ["c"]

def test_11_blocking_retrieval_w_timeout(server):
    print("\n[tester] Testing #S16: Blocking Retrieval with Timeout")
    s1 = socket.create_connection(("localhost", 6379))
    
    print("[tester] [client-1] > BLPOP blfoo1 1")
    print("[tester] Waiting...")
    s1.sendall(to_resp_array("BLPOP blfoo1 1"))
    
    time.sleep(1.1)
    resp1 = s1.recv(1024)
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == None
    
    print("[tester] [client-1] > BLPOP blfoo1 1")
    print("[tester] Waiting...")
    s1.sendall(to_resp_array("BLPOP blfoo1 1"))
    
    time.sleep(0.5)
    resp = send_command("RPUSH blfoo1 a")
    print("[tester] [client-2] > RPUSH blfoo a")
    parsed = parse_resp(resp)
    print(f"[tester] [client-2] Received {parsed}")
    
    resp1 = s1.recv(1024)
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == ["blfoo1", "a"]