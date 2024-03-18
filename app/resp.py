from app.constants import ENCODING
from base64 import b64decode
from socket import socket

class RESPSocket:
    def __init__(self, socket: socket, addr: tuple[str, int]) -> None:
        self._socket = socket
        self._addr = addr
     
    def sendall(self, payload: bytes) -> None:
        self._socket.sendall(payload)

    def get_addr(self) -> tuple[str, int]:
        return self._addr

class NullSocket(RESPSocket):
    def __init__(self) -> None:
        pass

    def sendall(self, payload: bytes) -> None:
        return None
    
    def getsockname(self) -> tuple[str, int]:
        return ("", -1)

class RESPBuffer:
    def __init__(self, buffer):
        self.buffer = buffer
    
    def read(self, num: int) -> bytes:
        b, self.buffer = self.buffer[:num], self.buffer[num:]
        return b
    
    def partition(self, delimiter: bytes):
        b, _, self.buffer = self.buffer.partition(delimiter)
        return b
    
    def is_not_empty(self) -> bool:
        return len(self.buffer) > 0

def decode(buffer: RESPBuffer):
    first_char = buffer.read(1)

    if first_char == b'*':
        n = int(buffer.partition(b'\r\n').decode(ENCODING))
        return [decode(buffer) for _ in range(n)]
    elif first_char == b'$':
        n = int(buffer.partition(b'\r\n'))
        bulk_string = buffer.read(n)
        
        if not bulk_string.startswith(b'REDIS0011') and buffer.read(2) != b'\r\n':
            raise Exception("Invalid RESP request")
            
        return bulk_string.decode(ENCODING)
    elif first_char == b'+':
        simple_string = buffer.partition(b'\r\n').decode(ENCODING)
        return simple_string
    else:
        raise Exception(f"Unknown RESP type: {first_char}")

def decode_command(buffer: RESPBuffer) -> tuple[str, list[str]]:
    decoded = decode(buffer)

    return decoded[0].upper(), decoded[1:]

def encode_simple_string(value: str) -> bytes:
    return f"+{value}\r\n".encode(ENCODING)

def encode_error_string(value: str) -> bytes:
    return f"-{value}\r\n".encode(ENCODING)

def encode_bulk_string(value: list[str]) -> bytes:
    if not value:
        return "$-1\r\n".encode(ENCODING)

    bulk_string = "\r\n".join(value)
        
    return f"${len(bulk_string)}\r\n{bulk_string}\r\n".encode(ENCODING)

def encode_array(value: list[object]) -> bytes:
    elements = ("\r\n").join([f"${len(str(s))}\r\n{s}" for s in value])

    return f"*{len(value)}\r\n{elements}\r\n".encode(ENCODING)

def encode_rdb_file(contents_b64: str) -> bytes:
    return f"${len(b64decode(contents_b64))}\r\n".encode(ENCODING) + b64decode(contents_b64)

def encode_integer(value: int) -> bytes:
    return f":{value}\r\n".encode(ENCODING)

def encode_stream(stream: list[list[str]]) -> bytes:
    stream_str = f"*{len(stream)}\r\n"

    for entry in stream:
        stream_str += "*2\r\n"
        entry_id, entry = entry[0], entry[1:]
        stream_str += f"${len(entry_id)}\r\n{entry_id}\r\n*{len(entry)}\r\n"

        for value in entry:
            stream_str += f"${len(value)}\r\n{value}\r\n"

    return stream_str.encode(ENCODING)