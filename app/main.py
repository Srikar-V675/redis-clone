import socket  # noqa: F401
import threading
import time 
from collections import deque
from typing import Dict
from .parser import RESPParser, RESPSerializer
from commands.core import RedisCommandHandler


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
            "LPOP": self.handle_lpop,
            "BLPOP": self.handle_blpop,
            "TYPE": self.handle_type,
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
    
    
    def handle_lpop(self, *args):
        key = args[0]
        num = int(args[1]) if len(args) == 2 else 1
        return self.datastore.lpop(key, num)
    
    
    def handle_blpop(self, *args):
        key = args[0]
        timeout = int(args[1])
        return self.datastore.blpop(key, timeout)
    
    
    def handle_type(self, *args):
        key = args[0]
        return self.datastore.type(key)
        

class DataStore:
    def __init__(self):
        self.STORE = {}
    
    
    def set(self, key, value, *options):
        try:
            self.STORE[key] = {
                "value": value,
                "expiry": None,
                "type": "string",
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
                start = 0
                if "blocking_clients" in self.STORE[key]:
                    for val in values:
                        if not self.STORE[key]["blocking_clients"]:
                            break
                        client = self.STORE[key]["blocking_clients"].popleft()
                        client["val"] = val
                        client["event"].set()
                        start += 1
                if start < len(values):
                    self.STORE[key]["value"].extend(values[start:])
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
    
    # TODO: check list type
    def lpop(self, key, num):
        try:
            if key in self.STORE:
                if len(self.STORE[key]["value"]) == 0:
                    return None
                else:
                    num = min(num, len(self.STORE[key]["value"]))
                    pop = [self.STORE[key]["value"].popleft() for _ in range(num)]
                    return pop[0] if num == 1 else pop
            else:
                return None
        except Exception as e:
            raise(e)
    
    # TODO: check if key is holdign list type
    def blpop(self, key, timeout=0):
        try:
            if key not in self.STORE or (key in self.STORE and not self.STORE[key]["value"]):
                waiter = {
                    "event": threading.Event(),
                    "val": None
                }
                if key not in self.STORE:
                    self.STORE[key] = {
                        "value": deque(),
                        "expiry": None, 
                        "type": "list",
                    }
                self.STORE[key].setdefault("blocking_clients", deque()).append(waiter)
                signaled = waiter["event"].wait(None if timeout == 0 else timeout)
                if not signaled:
                    self.STORE[key]["blocking_clients"].remove(waiter)
                    return None
                else:
                    return [key, waiter["val"]]
            else:
                return [key, self.STORE[key]["value"].popleft()]
        except Exception as e:
            raise(e)
    
    
    def type(self, key):
        try:
            if key in self.STORE:
                return self.STORE[key]["type"]
            else:
                return "none"
        except Exception as e:
            raise(e)


class Server:
    def __init__(self):
        self.server_socket: socket = socket.create_server(("localhost", 6379), reuse_port=True)
        self.db: Dict= {}
        self.cmd_handler: RedisCommandHandler = RedisCommandHandler(db=self.db)
    
    def start(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"New connection from: {addr}")
            thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            thread.start()
    
    def handle_client(self, client_socket: socket):
        try:
            while True:
                raw = client_socket.recv(1024)
            
                if not raw:
                    break
            
                tokens = RESPParser.parse_request(raw)
                if tokens is None:
                    error_response = RESPSerializer.serialize_error("ERR Invalid command format")
                    client_socket.sendall(error_response)
                else:
                    response = self.cmd_handler.handle_command(tokens)
                    client_socket.sendall(response)
        except Exception as e:
            print("\n Unexpected Error:", str(e))
        finally:
            client_socket.close()
                

def main():
    print("Logs from your program will appear here!")
    
    redis_server = Server()
    redis_server.start()
    

if __name__ == "__main__":
    main()
