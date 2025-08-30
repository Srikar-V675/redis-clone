import socket  # noqa: F401
import threading
import time 
from collections import deque


class Parser:
    def __init__(self):
        pass
    
    
    @staticmethod
    def parse_request(request: bytes):
        data = request.decode().split("\r\n")
        num_elements = int(data[0][1:])
        tokens = []
        
        i = 1
        while i < len(data) and len(tokens) < num_elements:
            if data[i].startswith("$"):
                length = int(data[i][1:])
                tokens.append(data[i+1])
                i += 2
            else:
                i += 1
        
        cmd, *args = tokens
        return cmd.upper(), args
    
    
    @staticmethod
    def construct_bulk_string(s: str):
        return f"${len(s)}\r\n{s}\r\n"
    
    
    @staticmethod
    def format_response(response):
        if response is None:
            return "$-1\r\n"
        elif isinstance(response, str):
            if response == "OK" or response == "PONG":
                return f"+{response}\r\n"
            else:
                return Parser.construct_bulk_string(s=response)
        elif isinstance(response, int):
            return f":{response}\r\n"
        elif isinstance(response, list):
            s = [f"*{len(response)}\r\n"]
            for resp in response:
                s.append(Parser.construct_bulk_string(s=resp))
            return ''.join(s)


class CommandHandler:
    def __init__(self, datastore):
        self.datastore = datastore
        self.commands = {
            "ECHO": self.handle_echo,
            "PING": self.handle_ping,
            "SET": self.handle_set,
            "GET": self.handle_get,
            "RPUSH": self.handle_rpush,
            "LPUSH": self.handle_lpush,
            "LRANGE": self.handle_lrange,
            "LLEN": self.handle_llen,
        }
    
    
    def execute(self, cmd, *args):
        if cmd in self.commands:
            return self.commands[cmd](*args)
    
    
    def handle_echo(self, message):
        return message
    
    
    def handle_ping(self, *args):
        return "PONG"
    
    
    def handle_set(self, *args):
        key = args[0]
        value = args[1]
        options = args[2:] if len(args) > 2 else ()
        return self.datastore.set(key, value, *options)
    
    
    def handle_get(self, *args):
        key = args[0]
        return self.datastore.get(key)
    
    
    def handle_rpush(self, *args):
        key, *values = args
        return self.datastore.rpush(key, *values)
    
    
    def handle_lpush(self, *args):
        key, *values = args
        return self.datastore.lpush(key, *values)
    
    
    def handle_lrange(self, *args):
        key = args[0]
        start = int(args[1])
        stop = int(args[2])
        return self.datastore.lrange(key, start, stop)
    
    
    def handle_llen(self, *args):
        key = args[0]
        return self.datastore.llen(key)
        

class DataStore:
    def __init__(self):
        self.STORE = {}
    
    
    def set(self, key, value, *options):
        try:
            self.STORE[key] = {
                "value": value,
                "expiry": None,
                "type": "str",
            }
            if options:
                if options[0].upper() == "PX":
                    self.STORE[key]["expiry"] = (
                        time.time() * 1000) + int(options[1]
                    )
            return "OK"
        except Exception as e:
            raise(e)
    
    
    def get(self, key):
        try:
            value = self.STORE[key]["value"]
            if self.STORE[key]["expiry"] and (time.time()*1000) > self.STORE[key]["expiry"]:
                del self.STORE[key]
                return None
            else:
                return value
        except Exception as e:
            raise(e)
    
    
    def rpush(self, key, *values):
        try:
            if key in self.STORE:
                self.STORE[key]["value"].extend(values)
            else:
                self.STORE[key] = {
                    "value": deque(values),
                    "expiry": None,
                    "type": "list",
                }
            return len(self.STORE[key]["value"])
        except Exception as e:
            raise(e)
    
    
    def lpush(self, key, *values):
        try:
            if key in self.STORE:
                self.STORE[key]["value"].extendleft(values)
            else:
                self.STORE[key] = {
                    "value": deque(reversed(values)),
                    "expiry": None, 
                    "type": "list",
                }
            return len(self.STORE[key]["value"])
        except Exception as e:
            raise(e)
    
    
    def lrange(self, key, start: int, stop: int):
        try:
            if key not in self.STORE:
                return []
            else:
                return list(self.STORE[key]["value"])[start:stop+1 or None] # -1+1 = 0 -> 0 or None -> None(last element)
        except Exception as e:
            raise(e)
    
    
    def llen(self, key):
        try:
            if key in self.STORE:
                return len(self.STORE[key]["value"])
            else:
                return 0
        except Exception as e:
            raise(e)


class Server:
    def __init__(self):
        self.server_socket: socket = socket.create_server(("localhost", 6379), reuse_port=True)
        self.datastore = DataStore()
        self.cmd_handler = CommandHandler(self.datastore)
    
    def start(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"New connection from: {addr}")
            thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            thread.start()
    
    def handle_client(self, client_socket: socket):
        with client_socket:
            while True:
                raw = client_socket.recv(1024)
                cmd, args = Parser.parse_request(request=raw)
                response = self.cmd_handler.execute(cmd, *args)
                print(response)
                client_socket.sendall(Parser.format_response(response).encode())
                

def main():
    print("Logs from your program will appear here!")
    
    redis_server = Server()
    redis_server.start()
    

if __name__ == "__main__":
    main()
