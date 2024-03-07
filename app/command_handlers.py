from socket import socket
from app.functions import encode_simple_string

def handle_ping(socket: socket, args: list[str]) -> None:
    encoded = encode_simple_string("PONG")
    socket.sendall(encoded)

