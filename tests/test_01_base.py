import socket
from helpers import send_command, to_resp_array, parse_resp

def test_01_ping(server):
    print("\n[tester] Testing #S1: Respond to PING")
    resp = send_command("PING")
    print("[tester] [client] > PING")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "PONG"

def test_02_multiple_pings(server):
    print("\n[tester] Testing #S2: Respond to Multiple PINGs")
    for i in range(3):
        resp = send_command("PING")
        print(f"[tester] [client] > PING ({i+1})")
        parsed = parse_resp(resp)
        print(f"[tester] [client] Received {parsed}")
        assert parsed == "PONG"
    
def test_03_concurrent_clients(server):
    print("\n[tester] Testing #S3: Handle Concurrent Clients")
    s1 = socket.create_connection(("localhost", 6379))
    s2 = socket.create_connection(("localhost", 6379))

    # Send PING from client 1
    s1.sendall(to_resp_array("PING"))
    resp1 = s1.recv(1024)
    print("[tester] [client-1] > PING")
    parsed1 = parse_resp(resp1)
    print(f"[tester] [client-1] Received {parsed1}")
    assert parsed1 == "PONG"

    # Send PING from client 2
    s2.sendall(to_resp_array("PING"))
    resp2 = s2.recv(1024)
    print("[tester] [client-2] > PING")
    parsed2 = parse_resp(resp2)
    print(f"[tester] [client-2] Received {parsed2}")
    assert parsed2 == "PONG"

    s1.close()
    s2.close()

def test_04_echo(server):
    print("\n[tester] Testing #S4: Implement ECHO Command")
    resp = send_command("ECHO hello")
    print("[tester] [client] > ECHO hello")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "hello"

def test_05_set_get(server):
    print("\n[tester] Testing #S5: Implement SET & GET Commands")
    resp = send_command("SET foo bar")
    print("[tester] [client] > SET foo bar")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "OK"
    
    resp = send_command("GET foo")
    print("[tester] [client] > GET foo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "bar"

def test_06_expiry(server):
    print("\n[tester] Testing #S6: Expiry")
    resp = send_command("SET foo bar px 100")
    print("[tester] [client] > SET foo bar px 100")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "OK"
    
    import time
    time.sleep(0.2)
    resp = send_command("GET foo")
    print("[tester] [client] > GET foo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == None