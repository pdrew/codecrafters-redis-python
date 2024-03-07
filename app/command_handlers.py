from socket import socket
from app.functions import encode_simple_string
from app.constants import ENCODING

def handle_ping(socket: socket, args: list[bytes]) -> None:
    encoded = encode_simple_string("PONG")
    socket.sendall(encoded)

def handle_echo(socket: socket, args: list[bytes]) -> None:
    message = ' '.join([b.decode(ENCODING) for b in args])
    encoded = encode_simple_string(message)
    socket.sendall(encoded)