from socket import socket
from app.functions import encode_simple_string, encode_bulk_string
from app.constants import ENCODING
from time import time

def handle_ping(socket: socket, args: list[bytes]) -> None:
    response = encode_simple_string("PONG")
    socket.sendall(response)

def handle_echo(socket: socket, args: list[bytes]) -> None:
    message = ' '.join([b.decode(ENCODING) for b in args])
    response = encode_simple_string(message)
    socket.sendall(response)

def handle_set(socket: socket, args: list[bytes], database: dict) -> None:
    key = args.pop(0).decode(ENCODING)
    value = args.pop(0).decode(ENCODING)

    expiry = (round(time() * 1000) + int(args[1].decode(ENCODING))) if args and args[0].upper() == b'PX' else None

    database[key] = (value, expiry)
    response = encode_simple_string("OK")
    socket.sendall(response)

def handle_get(socket: socket, args: list[bytes], database: dict) -> None:
    key = args[0].decode(ENCODING)

    response = encode_bulk_string(None)

    if key in database:
        value, expires = database[key]

        current = round(time() * 1000)

        if expires and expires < current:
            del database[key]
        else:
            response = encode_simple_string(value)
    
    socket.sendall(response)

def handle_info(socket: socket, args: list[bytes]) -> None:
    socket.sendall(encode_bulk_string(["role:master"]))