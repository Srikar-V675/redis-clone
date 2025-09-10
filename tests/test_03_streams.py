from helpers import send_command, parse_resp

def test_01_type(server):
    print("\n[tester] Testing #S17: TYPE Command")
    resp = send_command("SET ktype foo")
    print("[tester] [client] > SET ktype foo")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    
    resp = send_command("TYPE ktype")
    print("[tester] [client] > TYPE ktype")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "string"
    
    resp = send_command("TYPE blah")
    print("[tester] [client] > TYPE blah")
    parsed = parse_resp(resp)
    print(f"[tester] [client] Received {parsed}")
    assert parsed == "none"