from io import BufferedReader
from struct import unpack
from app.constants import *

class Database():
    def __init__(self) -> None:
        self._data = {}
    
    def set(self, key: str, value: tuple[str, int | None]) -> None:
        self._data[key] = value

    def get(self, key: str) -> tuple[str, int | None]:
        return self._data[key]
    
    def delete(self, key: str) -> None:
        del self._data[key]

    def contains(self, key: str) -> bool:
        return key in self._data
    
    def keys(self) -> list[str]:
        return self._data.keys()
    
class Database():
    def __init__(self, filename: str | None) -> None:
        self._data = {}

        if filename:
            self._read_rdb(filename)

    def set(self, key: str, value: tuple[str, int | None]) -> None:
        self._data[key] = value

    def get(self, key: str) -> tuple[str, int | None]:
        return self._data[key]
    
    def delete(self, key: str) -> None:
        del self._data[key]

    def contains(self, key: str) -> bool:
        return key in self._data
    
    def keys(self) -> list[str]:
        return self._data.keys()
    
    def _read_rdb(self, filename: str) -> None:
        with open(filename, "rb") as file:
            self._check_magic_string(file)
            self._check_version(file)
            while True:
                try:
                    self._parse_next(file)
                except StopIteration:
                    break

    def _check_magic_string(self, file: BufferedReader) -> None:
        if file.read(5) != b'REDIS':
            raise Exception("Invalid file format")
        
    def _check_version(self, file: BufferedReader) -> None:
        version_str = file.read(4)
        version = int(version_str)
        if version < 1:
            raise Exception(f"Invalid version: {version}")
        
    def _parse_next(self, file: BufferedReader) -> None:
        opcode = file.read(1)

        match opcode:
            case b'\xFA':
                self._parse_aux(file)
            case b"\xFB":
                self._parse_resizedb(file)
            case b"\xFE":
                self._parse_selectdb(file)
            case b'\xFD':
                expiry_s = self._parse_expiry_s(file)
                file.read(1)
                self._parse_key_value_pair(file, expiry_s * 1000)
            case b'\xFC':
                expiry_ms = self._parse_expiry_ms(file)
                file.read(1)
                self._parse_key_value_pair(file, expiry_ms)
            case b"\x00":
                self._parse_key_value_pair(file, None)
            case b'\xFF':
                file.read(8)
                raise StopIteration
    
    def _parse_aux(self, file: BufferedReader) -> None:
        key = self._parse_string(file)
        value = self._parse_string(file)
        print(f"AUX {key}: {value}")

    def _parse_resizedb(self, file: BufferedReader) -> None:
        db_size = self._parse_length(file)
        expiry_size = self._parse_length(file)

        print(f"db_size: {db_size}")
        print(f"expiry_size: {expiry_size}")

    def _parse_selectdb(self, file: BufferedReader) -> None:
        db_number = self._parse_length(file)

        print(f"db_number: {db_number}")

    def _parse_expiry_ms(self, file: BufferedReader) -> int:
        bytes = file.read(8)
        expiry = unpack('<Q', bytes)[0]

        return expiry
    
    def _parse_expiry_s(self, file: BufferedReader) -> int:
        bytes = file.read(4)
        expiry = unpack('<I', bytes)[0]

        return expiry

    def _parse_key_value_pair(self, file: BufferedReader, expiry_ms: int | None) -> None:
        key = self._parse_string(file)
        value = self._parse_string(file)

        self._data[key] = (value, expiry_ms)

    def _parse_string(self, file: BufferedReader) -> str | int:
        length = self._parse_length(file)

        if length > 0:
            return file.read(length).decode()
        
        byte = file.read(1)
        return unpack("B", byte)[0]

    def _parse_length(self, file: BufferedReader) -> int:
        byte = file.read(1)
        
        length = unpack("B", byte)[0]
        bits = length >> 6

        return length if bits == 0b00 else -1