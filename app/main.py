import socket  # noqa: F401
from threading import Thread
from typing import Dict
from app.parser import RESPParser, RESPSerializer
from app.commands.handler import RedisCommandHandler


class Server:
    def __init__(self):
        self.server_socket: socket = socket.create_server(("localhost", 6379), reuse_port=True)
        self.db: Dict = {}
        self.waiting_clients: Dict = {}
        self.cmd_handler: RedisCommandHandler = RedisCommandHandler(db=self.db, waiting_clients=self.waiting_clients)
    
    def start(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"New connection from: {addr}")
            thread = Thread(target=self.handle_client, args=(client_socket,))
            thread.start()
    
    def handle_client(self, client_socket: socket):
        # try:
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
        # except Exception as e:
        #     print("\n Unexpected Error:", str(e))
        # finally:
        #     client_socket.close()
                

def main():
    print("Logs from your program will appear here!")
    
    redis_server = Server()
    redis_server.start()
    

if __name__ == "__main__":
    main()
