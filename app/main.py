import socket  # noqa: F401
import threading


def handle_client(connection: socket):
    while True:
        data = connection.recv(1024)
        if not data: break
        connection.sendall(b"+PONG\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    
    # server
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    # accepting connections: new thread for connection
    while True:
        conn, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(conn,))
        thread.start()
    

if __name__ == "__main__":
    main()
