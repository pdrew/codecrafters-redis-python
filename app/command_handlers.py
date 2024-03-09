from socket import socket
from app.resp import *
from app.constants import *
from time import time

def handle_ping(socket: RESPSocket, args: list[str]) -> None:
    response = encode_simple_string("PONG")
    socket.sendall(response)

def handle_echo(socket: RESPSocket, args: list[str]) -> None:
    message = ' '.join([s for s in args])
    response = encode_simple_string(message)
    socket.sendall(response)

def handle_set(socket: RESPSocket, args: list[str], database: dict) -> None:
    key = args[0]
    value = args[1]

    expiry = (round(time() * 1000) + int(args[3])) if len(args) > 3 and args[2].upper() == 'PX' else None

    database[key] = (value, expiry)
    response = encode_simple_string("OK")
    socket.sendall(response)

def handle_get(socket: RESPSocket, args: list[str], database: dict) -> None:
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

def handle_info(socket: RESPSocket, args: list[str], config: dict) -> None:
    info = [f"{ROLE}:{config[ROLE]}"]
    
    if config[ROLE] is LEADER_ROLE:
        info.append(f"{REPLID}:{config[REPLID]}")
        info.append(f"{REPLOFFSET}:{config[REPLOFFSET]}")

    socket.sendall(encode_bulk_string(info))

def handle_replconf(socket: RESPSocket, args: list[str], config: dict) -> None:
    if len(args) > 1 and args[0].upper() == "GETACK":
        socket.sendall(encode_array(["REPLCONF", "ACK", config[REPLOFFSET]]))
    elif config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string("OK"))

def handle_psync(socket: RESPSocket, args: list[str], config: dict[str, str|int], replicas: dict[socket, int]) -> None:
    if config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string(f"FULLRESYNC {config[REPLID]} {config[REPLOFFSET]}"))
        socket.sendall(encode_rdb_file(EMPTY_RDB_FILE_B64))
        print(f"adding replica: {socket.getsockname()}")
        replicas.setdefault(socket, 0)
    
def handle_wait(socket: RESPSocket, args: list[str]) -> None:
    socket.sendall(encode_integer(0))