from socket import socket
from app.functions import encode_simple_string, encode_bulk_string, encode_rdb_file
from app.constants import ENCODING, ROLE, LEADER_ROLE, REPLID, REPLOFFSET, EMPTY_RDB_FILE_B64
from time import time

def handle_ping(socket: socket, args: list[str]) -> None:
    response = encode_simple_string("PONG")
    socket.sendall(response)

def handle_echo(socket: socket, args: list[str]) -> None:
    message = ' '.join([s for s in args])
    response = encode_simple_string(message)
    socket.sendall(response)

def handle_set(socket: socket, args: list[str], database: dict) -> None:
    key = args[0]
    value = args[1]

    expiry = (round(time() * 1000) + int(args[3])) if len(args) > 3 and args[2].upper() == 'PX' else None

    database[key] = (value, expiry)
    response = encode_simple_string("OK")
    socket.sendall(response)

def handle_get(socket: socket, args: list[str], database: dict) -> None:
    key = args[0]

    response = encode_bulk_string(None)

    if key in database:
        value, expires = database[key]

        current = round(time() * 1000)

        if expires and expires < current:
            del database[key]
        else:
            response = encode_simple_string(value)
    
    socket.sendall(response)

def handle_info(socket: socket, args: list[str], config: dict) -> None:
    info = [f"{ROLE}:{config[ROLE]}"]
    
    if config[ROLE] is LEADER_ROLE:
        info.append(f"{REPLID}:{config[REPLID]}")
        info.append(f"{REPLOFFSET}:{config[REPLOFFSET]}")

    socket.sendall(encode_bulk_string(info))

def handle_replconf(socket: socket, args: list[str], config: dict) -> None:
    if config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string("OK"))

def handle_psync(socket: socket, args: list[str], config: dict[str, str|int], replicas: dict[socket, int]) -> None:
    if config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string(f"FULLRESYNC {config[REPLID]} {config[REPLOFFSET]}"))
        socket.sendall(encode_rdb_file(EMPTY_RDB_FILE_B64))
        print(f"adding replica: {socket.getsockname()}")
        replicas.setdefault(socket, 0)