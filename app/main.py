import socket  # noqa: F401
import threading
import time 


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
    def format_response(response):
        if response is None:
            return "$-1\r\n"
        elif isinstance(response, str):
            if response == "OK" or response == "PONG":
                return f"+{response}\r\n"
            else:
                return f"${len(response)}\r\n{response}\r\n"
        elif isinstance(response, int):
            return f":{response}\r\n"
        elif isinstance(response, list):
            pass
        else:
            return "+PONG\r\n"


class CommandHandler:
    def __init__(self, datastore):
        self.datastore = datastore
        self.commands = {
            "ECHO": self.handle_echo,
            "PING": self.handle_ping,
            "SET": self.handle_set,
            "GET": self.handle_get,
            "RPUSH": self.handle_rpush,
        }
    
    
    def execute(self, cmd, *args):
        if cmd in self.commands:
            return self.commands[cmd](*args)
        else:
            return f"-ERR unknown command '{cmd}'"
    
    
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
        

class DataStore:
    def __init__(self):
        self.DATA = {}
        self.EXPIRY = {}
        self.LIST = {}
    
    
    def set(self, key, value, *options):
        try:
            self.DATA[key] = value
            print("Options:", options)
            if options:
                if options[0].upper() == "PX":
                    self.EXPIRY[key] = (
                        time.time() * 1000) + int(options[1]
                    )
            return "OK"
        except Exception as e:
            raise(e)
    
    
    def get(self, key):
        try:
            value = self.DATA[key]
            if key in self.EXPIRY and (time.time()*1000) > self.EXPIRY[key]:
                del self.DATA[key]
                del self.EXPIRY[key]
                return None
            else:
                return value
        except Exception as e:
            raise(e)
    
    
    def rpush(self, key, *values):
        try:
            if key in self.LIST:
                self.LIST[key].extend(values)
            else:
                self.LIST[key] = list(values)
            return len(self.LIST[key])
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
