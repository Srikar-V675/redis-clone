import socket  # noqa: F401
import threading

DATA = {}

def parser(connection: socket):
    request = connection.recv(1024)
    data = request.decode().splitlines()
    parsed_data = []
    for i in range(2, len(data), 2):
        parsed_data.append(data[i])
    return parsed_data


def handle_client(connection: socket):
    while True:
        parsed_data = parser(connection=connection)
        if parsed_data[0].lower() == "echo":
            s = parsed_data[1]
            connection.sendall(f"${len(s)}\r\n{s}\r\n".encode())
        elif parsed_data[0].lower() == "set":
            DATA[parsed_data[1]] = parsed_data[2]
            connection.sendall(b"+OK\r\n")
        elif parsed_data[0].lower() == "get":
            val = DATA[parsed_data[1]]
            connection.sendall(f"${len(val)}\r\n{val}\r\n".encode())
        else:
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
