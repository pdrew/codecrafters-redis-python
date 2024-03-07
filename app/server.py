from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread
from app.functions import *
from app.command_handlers import *
from app.constants import *

class Server:
    def __init__(self, config: dict) -> None:
        self._config = config
        self._database = {}
        self._replicas = {}
        self._handlers = {
            'PING': [handle_ping],
            'ECHO': [handle_echo],
            'SET': [lambda socket, args: handle_set(socket, args, self._database)],
            'GET': [lambda socket, args: handle_get(socket, args, self._database)],
            'INFO': [lambda socket, args: handle_info(socket, args, self._config)],
            'REPLCONF': [lambda socket, args: handle_replconf(socket, args, self._config)],
            'PSYNC': [lambda socket, args: handle_psync(socket, args, self._config, self._replicas)]
        }

    def start(self, port: int) -> None:
        if self._config[ROLE] is FOLLOWER_ROLE:
            self._send_handshake(port)

        with socket(AF_INET, SOCK_STREAM) as s:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            s.bind(('', port))
            s.listen()

            print(f"Listening on port {port}")

            while True:
                client_socket, addr = s.accept()

                thread = Thread(target=self._on_client_request, args=(client_socket, addr), daemon=True)
                thread.start()

    def _send_handshake(self, port: int) -> None:
        s = socket(AF_INET, SOCK_STREAM)
        s.connect((self._config[LEADER_HOST], self._config[LEADER_PORT]))

        payload = [
            ["PING"],
            ["REPLCONF", "listening-port", port],
            ["REPLCONF", "capa", "psync2"],
        ]

        for p in payload:
            s.sendall(encode_array(p))
            response = s.recv(1024)
            print(response)

        s.sendall(encode_array(["PSYNC", "?", "-1"]))

        thread = Thread(target=self._on_leader_request, args=(s,), daemon=True)
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

                if command not in self._handlers:
                    continue

                for handler in self._handlers[command]:
                    handler(client_socket, args)

                if command in WRITE_COMMANDS:
                    for i, replica_socket in enumerate(self._replicas):
                        print(f"propagating {command} command to replica {i}: {replica_socket.getsockname()}")
                        print(encode_array([command] + args))
                        replica_socket.sendall(encode_array([command] + args))

        print("closing client socket")
        client_socket.close()

    def _on_leader_request(self, leader_socket: socket) -> None:
        while True:
            data = leader_socket.recv(1024)

            if len(data) == 0:
                break
            
            print(f"received request from leader {leader_socket.getsockname()}: {data}")

            buffer = RESPBuffer(data)

            while buffer.is_not_empty():
                command, args = decode_command(buffer)

                if command not in self._handlers:
                    continue

                for handler in self._handlers[command]:
                    handler(leader_socket, args)

        print("closing leader socket")
        leader_socket.close()