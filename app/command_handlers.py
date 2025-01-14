from app.resp import *
from app.constants import *
from time import time, sleep
from app.database import Database
import sys

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
    key, entry_id, entry = args[0], args[1], args[1:]

    if entry_id == "*":
        ms_time, seq_no = int(time()) * 1000, 0
        entry_id = f"{ms_time}-{seq_no}"
        entry[0] = entry_id
    else:
        ms_time, _, seq_no = entry_id.partition("-")
        ms_time, seq_no = int(ms_time), int(seq_no) if seq_no.isdigit() else seq_no

    if ms_time < 0 or (isinstance(seq_no, int) and seq_no < 0) or (ms_time == 0 and isinstance(seq_no, int) and seq_no == 0):
        socket.sendall(encode_error_string("ERR The ID specified in XADD must be greater than 0-0"))
        return

    if database.contains(key):
        stream, _ = database.get(key) 
        last_entry = stream[-1]

        last_ms_time, _, last_seq_no = last_entry[0].partition("-")
        last_ms_time, last_seq_no = int(last_ms_time), int(last_seq_no)

        if seq_no == "*":
            seq_no = last_seq_no + 1 if ms_time == last_ms_time else 0
            entry_id = f"{ms_time}-{seq_no}"
            entry[0] = entry_id

        if ms_time < last_ms_time or (ms_time == last_ms_time and seq_no <= last_seq_no):
            socket.sendall(encode_error_string("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
            return

        stream = stream + [entry]
    else:
        if seq_no == "*":
            seq_no = 1 if ms_time == 0 else 0
            entry_id = f"{ms_time}-{seq_no}"
            entry[0] = entry_id
        stream = [entry]

    database.set(key, (stream, None))

    socket.sendall(encode_bulk_string([entry_id]))

def handle_xrange(socket: RESPSocket, args: list[str], database: Database) -> None:
    key, start, end = args

    if start == "-":
        start_ms_time, start_seq_no = 0, 0
    else:
        start_ms_time, _, start_seq_no = start.partition("-")
        start_ms_time, start_seq_no = int(start_ms_time), int(start_seq_no) if start_seq_no.isdigit() else 0

    if end == "+":
        end_ms_time, end_seq_no = sys.maxsize, sys.maxsize
    else:
        end_ms_time, _, end_seq_no = end.partition("-")
        end_ms_time, end_seq_no = int(end_ms_time), int(end_seq_no) if end_seq_no.isdigit() else sys.maxsize

    if database.contains(key):
        stream, _ = database.get(key)
        
        stream_range = []

        for entry in stream:
            entry_id = entry[0]
            ms_time, seq_no = [int(e) for e in entry_id.split("-")]

            if ms_time >= start_ms_time and seq_no >= start_seq_no and ms_time <= end_ms_time and seq_no <= end_seq_no:
                stream_range.append(entry)
        
        socket.sendall(encode_stream(stream_range))
    else:
        socket.sendall(encode_bulk_string(None))

def handle_xread(socket: RESPSocket, args: list[str], database: Database) -> None:
    if args[0].upper() == "BLOCK":
        _, wait_ms, xread_args = args[0], args[1], args[3:]
    else:
        _, wait_ms, xread_args = args[0], None, args[1:]

    if len(xread_args) % 2 != 0:
        socket.sendall(encode_error_string("ERR invalid number of arguments for XREAD"))
        return 

    n = len(xread_args) // 2
    keys = xread_args[:n]
    starts = xread_args[n:]

    last_response_length, response = -1, b""

    timeout = (time() * 1000)
    
    if wait_ms and wait_ms.isdigit():
        wait_ms = int(wait_ms)
        timeout += wait_ms

    def should_wait() -> bool:
        if wait_ms == 0: 
            if last_response_length > -1 and len(response) > last_response_length:
                return False
            else:
                return True
        return (time() * 1000) < timeout

    while True:
        for i in range(n):
            key, start = keys[i], starts[i]

            if start == "$":
                start_ms_time, start_seq_no = 0, 0
            else:   
                start_ms_time, _, start_seq_no = start.partition("-")
                start_ms_time, start_seq_no = int(start_ms_time), int(start_seq_no) if start_seq_no.isdigit() else 0

            if database.contains(key):
                stream, _ = database.get(key)

                if start == "$":
                    entry = stream[-1]
                    starts[i] = entry[0]
                    continue

                stream_range = []

                for entry in stream:
                    entry_id = entry[0]
                    ms_time, seq_no = [int(e) for e in entry_id.split("-")]

                    if ms_time > start_ms_time or (ms_time == start_ms_time and seq_no > start_seq_no):
                        stream_range.append(entry)
                
                if stream_range:
                    response += f"*2\r\n${len(key)}\r\n{key}\r\n".encode(ENCODING) + encode_stream(stream_range)
    
        if should_wait():
            last_response_length, response = len(response), b""
            sleep(0.1)
        else:
            break
            
    response = f"*{n}\r\n".encode(ENCODING) + response if response else encode_bulk_string(None)

    socket.sendall(response)

