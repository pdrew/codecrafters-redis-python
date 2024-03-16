from app.resp import *
from app.constants import *
from time import time, sleep
from app.database import Database

def handle_ping(socket: RESPSocket, args: list[str]) -> None:
    response = encode_simple_string("PONG")
    socket.sendall(response)

def handle_echo(socket: RESPSocket, args: list[str]) -> None:
    message = ' '.join([s for s in args])
    response = encode_simple_string(message)
    socket.sendall(response)

def handle_set(socket: RESPSocket, args: list[str], database: Database) -> None:
    key = args[0]
    value = args[1]

    expiry = (round(time() * 1000) + int(args[3])) if len(args) > 3 and args[2].upper() == 'PX' else None

    database.set(key, (value, expiry))
    response = encode_simple_string("OK")
    socket.sendall(response)

def handle_get(socket: RESPSocket, args: list[str], database: Database) -> None:
    key = args[0]

    response = encode_bulk_string(None)

    if database.contains(key):
        value, expires = database.get(key)

        current = round(time() * 1000)

        if expires and expires < current:
            database.delete(key)
        else:
            response = encode_simple_string(value)
    
    socket.sendall(response)

def handle_info(socket: RESPSocket, args: list[str], config: dict) -> None:
    info = [f"{ROLE}:{config[ROLE]}"]
    
    if config[ROLE] is LEADER_ROLE:
        info.append(f"{REPLID}:{config[REPLID]}")
        info.append(f"{REPLOFFSET}:{config[REPLOFFSET]}")

    socket.sendall(encode_bulk_string(info))

def handle_replconf(socket: RESPSocket, args: list[str], config: dict, replicas: dict[tuple[str, int], tuple[RESPSocket, int]]) -> None:
    if len(args) > 1 and args[0].upper() == "GETACK":
        socket.sendall(encode_array(["REPLCONF", "ACK", config[REPLOFFSET]]))
    elif len(args) > 1 and args[0].upper() == "ACK":
        if socket.get_addr() in replicas:
            replicas[socket.get_addr()] = (socket, int(args[1]))
    elif config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string("OK"))

def handle_psync(socket: RESPSocket, args: list[str], config: dict[str, str|int], replicas: dict[tuple[str, int], tuple[RESPSocket, int]]) -> None:
    if config[ROLE] is LEADER_ROLE:
        socket.sendall(encode_simple_string(f"FULLRESYNC {config[REPLID]} {config[REPLOFFSET]}"))
        socket.sendall(encode_rdb_file(EMPTY_RDB_FILE_B64))
        print(f"adding replica: {socket.get_addr()}")
        replicas.setdefault(socket.get_addr(), (socket, 0))
    
def handle_wait(socket: RESPSocket, args: list[str], config: dict[str, str|int], replicas: dict[tuple[str, int], tuple[RESPSocket, int]]) -> None:
    wait_ms = int(args[1]) if len(args) > 1 else 0
    timeout = (time() * 1000) + wait_ms
    within_timeout = lambda : (time() * 1000) < timeout if wait_ms > 0 else True

    acknowledged = 0
    min_acknowledged = int(args[0])

    for replica_socket, _ in replicas.values():
        replica_socket.sendall(encode_array(["REPLCONF", "GETACK", "*"]))

    while within_timeout():
        acknowledged = sum([1 if offset >= config[REPLOFFSET] else 0 for _, offset in replicas.values()])
        print(f"{acknowledged} of {len(replicas)} acknowledged. Min: {min_acknowledged}")

        if acknowledged >= min(min_acknowledged, len(replicas)):
            break
        
        sleep(0.1)

    socket.sendall(encode_integer(acknowledged))

def handle_config(socket: RESPSocket, args: list[str], config: dict[str, str|int])-> None:
    if len(args) > 1 and args[0].upper() == "GET":
        key = args[1].lower()
        value = config[key] if key in [RDB_DIR, RDB_FILENAME] else ""
        socket.sendall(encode_array([key, value]))

def handle_keys(socket: RESPSocket, args: list[str], database: Database) -> None:
    keys = encode_array(database.keys())
    socket.sendall(keys)

def handle_type(socket: RESPSocket, args: list[str], database: Database) -> None:
    key = args[0]

    if database.contains(key):
        value = database.get(key)
        value_type = "string" if isinstance(value[0], str) else "stream"
        socket.sendall(encode_simple_string(value_type))
    else:
        socket.sendall(encode_simple_string("none"))

def handle_xadd(socket: RESPSocket, args: list[str], database: Database) -> None:
    key, stream = args[0], args[1:]

    if database.contains(key):
        stream = database.get(key) + stream

    database.set(key, (stream, None))

    socket.sendall(encode_bulk_string([args[1]]))