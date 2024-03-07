from socket import socket
from app.functions import encode_simple_string, encode_bulk_string
from app.constants import ENCODING

def handle_ping(socket: socket, args: list[bytes]) -> None:
    encoded = encode_simple_string("PONG")
    socket.sendall(encoded)

def handle_echo(socket: socket, args: list[bytes]) -> None:
    message = ' '.join([b.decode(ENCODING) for b in args])
    encoded = encode_simple_string(message)
    socket.sendall(encoded)

def handle_set(socket: socket, args: list[bytes], database: dict) -> None:
    key = args[0].decode(ENCODING)
    value =  ' '.join([b.decode(ENCODING) for b in args[1:]])
    database[key] = value
    encoded = encode_simple_string("OK")
    socket.sendall(encoded)

def handle_get(socket: socket, args: list[bytes], database: dict) -> None:
    key = args[0].decode(ENCODING)

    value = encode_simple_string(database[key]) if key in database else encode_bulk_string(None)

    socket.sendall(value)