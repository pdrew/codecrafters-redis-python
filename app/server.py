from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread
from app.functions import *
from app.command_handlers import *

class Server:
    def __init__(self) -> None:
        self.database = {}

        self.handlers = {
            b'PING': [handle_ping],
            b'ECHO': [handle_echo],
            b'SET': [lambda socket, args: handle_set(socket, args, self.database)],
            b'GET': [lambda socket, args: handle_get(socket, args, self.database)],
            b'INFO': [handle_info]
        }

    def start(self, port: int) -> None:
        with socket(AF_INET, SOCK_STREAM) as s:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            s.bind(('', port))
            s.listen()

            print(f"Listening on port {port}")

            while True:
                client_socket, addr = s.accept()

                thread = Thread(target=self._on_client_request, args=(client_socket, addr), daemon=True)
                thread.start()

    def _on_client_request(self, client_socket: socket, addr: tuple[str, int]) -> None:
        while True: 
            data = client_socket.recv(1024)

            if len(data) == 0:
                break

            print(f"received request from client {addr}: {data}")

            buffer = RESPBuffer(data)

            while buffer.is_not_empty():
                command, args = decode_command(buffer)

                for handler in self.handlers[command]:
                    handler(client_socket, args)

        print("closing client socket")
        client_socket.close()



