from app.constants import ENCODING

class RESPBuffer:
    def __init__(self, buffer):
        self.buffer = buffer
    
    def read(self, num: int) -> bytes:
        b, self.buffer = self.buffer[:num], self.buffer[num:]
        return b
    
    def partition(self, delimiter: bytes):
        b, _, self.buffer = self.buffer.partition(delimiter)
        return b

def decode(buffer: RESPBuffer):
    first_char = buffer.read(1)

    if first_char == b'*':
        n = int(buffer.partition(b'\r\n').decode())
        return [decode(buffer) for _ in range(n)]
                    
    elif first_char == b'$':
        n = int(buffer.partition(b'\r\n'))
        bulk_string = buffer.read(n)
        
        if buffer.read(2) != b'\r\n':
            raise Exception("Invalid RESP request")
            
        return bulk_string
    else:
        raise Exception(f"Unknown RESP type: {first_char}")

def encode_simple_string(obj: object) -> bytes:
    return f"+{obj}\r\n".encode(ENCODING)

def decode_command(buffer: RESPBuffer) -> tuple[str, list[str]]:
    decoded = decode(buffer)

    return decoded[0].upper(), decoded[1:]

