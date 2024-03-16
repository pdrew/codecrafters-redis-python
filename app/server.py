from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread
from app.resp import *
from app.command_handlers import *
from app.constants import *
from app.database import Database

class Server:
    def __init__(self, config: dict, database: Database) -> None:
        self._config = config
        self._database = database
        self._replicas = {}
        self._handlers = {
            'PING': [handle_ping],
            'ECHO': [handle_echo],
            'SET': [lambda socket, args: handle_set(socket, args, self._database)],
            'GET': [lambda socket, args: handle_get(socket, args, self._database)],
            'INFO': [lambda socket, args: handle_info(socket, args, self._config)],
            'REPLCONF': [lambda socket, args: handle_replconf(socket, args, self._config, self._replicas)],
            'PSYNC': [lambda socket, args: handle_psync(socket, args, self._config, self._replicas)],
            'WAIT': [lambda socket, args: handle_wait(socket, args, self._config, self._replicas)],
            'CONFIG': [lambda socket, args: handle_config(socket, args, self._config)],
            'KEYS': [lambda socket, args: handle_keys(socket, args, self._database)],
            'TYPE': [lambda socket, args: handle_type(socket, args, self._database)],
            'XADD': [lambda socket, args: handle_xadd(socket, args, self._database)]
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
            resp_socket = RESPSocket(client_socket, addr)

            while buffer.is_not_empty():
                command, args = decode_command(buffer)

                if command not in self._handlers:
                    continue

                for handler in self._handlers[command]:
                    handler(resp_socket, args)

                if command in WRITE_COMMANDS and self._replicas:
                    for replica_socket, _ in self._replicas.values():
                        print(f"propagating {command} command to replica {replica_socket.get_addr()}")
                        replica_socket.sendall(encode_array([command] + args))
                    
                    offset_increment = len(encode_array([command] + args))
                    print(f"incrementing leader offset {self._config[REPLOFFSET]} by {offset_increment}")
                    self._config[REPLOFFSET] += offset_increment

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

                resp_socket = RESPSocket(leader_socket, leader_socket.getsockname()) if command in LEADER_COMMANDS else NullSocket() 

                for handler in self._handlers[command]:
                    handler(resp_socket, args)

                offset_increment = len(encode_array([command] + args))
                print(f"incrementing offset {self._config[REPLOFFSET]} by {offset_increment}")
                self._config[REPLOFFSET] += offset_increment

        print("closing leader socket")
        leader_socket.close()