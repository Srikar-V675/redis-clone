import socket

def to_resp_array(command: str) -> bytes:
    """
    Convert a command like 'PING' or 'SET foo bar'
    into a RESP array.
    """
    parts = command.split()
    resp = f"*{len(parts)}\r\n"
    for p in parts:
        resp += f"${len(p)}\r\n{p}\r\n"
    return resp.encode()

def parse_resp(resp: bytes):
    """
    Parse a Redis RESP reply into a Python type.
    Supports simple strings, errors, integers, bulk strings, and arrays.
    """
    if not resp:
        return None

    prefix = chr(resp[0])
    rest = resp[1:].decode(errors="ignore")

    if prefix == "+":  # Simple String
        return rest.strip()
    elif prefix == "-":  # Error
        return f"(error) {rest.strip()}"
    elif prefix == ":":  # Integer
        return int(rest.strip())
    elif prefix == "$":  # Bulk String
        length, _, data = rest.partition("\r\n")
        length = int(length)
        if length == -1:
            return None
        return data[:length]
    elif prefix == "*":  # Array
        lines = rest.split("\r\n")
        arr = []
        i = 0
        while i < len(lines) - 1:
            if lines[i].startswith("$"):
                l = int(lines[i][1:])
                if l == -1:
                    arr.append(None)
                else:
                    arr.append(lines[i + 1])
                i += 2
            else:
                i += 1
        return arr
    else:
        return resp.decode(errors="ignore")


def send_command(cmd: str, host="localhost", port=6379) -> bytes:
    """
    Send a command (text, not RESP) to the server and return raw RESP reply.
    """
    with socket.create_connection((host, port)) as sock:
        sock.sendall(to_resp_array(cmd))
        return sock.recv(4096)
