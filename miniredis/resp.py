"""
RESP2 (REdis Serialization Protocol) parser and encoder, from scratch.

Wire format:
    +OK\r\n                   simple string
    -ERR message\r\n          error
    :42\r\n                   integer
    $5\r\nhello\r\n           bulk string   ($-1\r\n = null)
    *2\r\n$3\r\nGET\r\n$1\r\nk\r\n   array  (*-1\r\n = null array)

Clients send commands as arrays of bulk strings. "Inline" commands (plain
text like `PING\r\n`, typed into telnet/netcat) are supported too.

The parser is incremental: TCP may deliver half a command or ten commands
in one read, so `RespParser.feed()` buffers bytes and yields every complete
command it can find.
"""

from typing import Iterator, Union

MAX_BULK_LEN = 512 * 1024 * 1024   # same limit as Redis
MAX_ARRAY_LEN = 1024 * 1024
MAX_INLINE_LEN = 64 * 1024


class ProtocolError(Exception):
    pass


class SimpleString(str):
    """Encoded as +value (vs. a normal str/bytes, which becomes a bulk string)."""


class Error(Exception):
    """Encoded as -message. Raised by command handlers to reply with an error."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NullArray:
    pass


OK = SimpleString("OK")
PONG = SimpleString("PONG")
QUEUED = SimpleString("QUEUED")
NULL_ARRAY = NullArray()

Reply = Union[None, int, bytes, str, list, SimpleString, Error, NullArray]


def encode(value: Reply) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, SimpleString):
        return b"+" + value.encode() + b"\r\n"
    if isinstance(value, Error):
        return b"-" + value.message.encode() + b"\r\n"
    if isinstance(value, bool):
        return b":%d\r\n" % int(value)
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, (bytes, bytearray)):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if isinstance(value, NullArray):
        return b"*-1\r\n"
    if isinstance(value, (list, tuple)):
        return b"*%d\r\n" % len(value) + b"".join(encode(v) for v in value)
    raise TypeError(f"cannot RESP-encode {type(value).__name__}")


def encode_command(*args) -> bytes:
    """Encode a command the way clients (and the AOF file) do: an array of bulk strings."""
    parts = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return encode(parts)


class _Incomplete(Exception):
    pass


class RespParser:
    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes) -> Iterator[list[bytes]]:
        """Add received bytes; yield each complete command (a list of bytes args)."""
        self._buf += data
        while self._buf:
            try:
                command, consumed = self._parse_command(0)
            except _Incomplete:
                return
            del self._buf[:consumed]
            if command:  # skip empty inline lines
                yield command

    @property
    def pending_bytes(self) -> int:
        return len(self._buf)

    def _read_line(self, pos: int) -> tuple[bytes, int]:
        end = self._buf.find(b"\r\n", pos)
        if end == -1:
            if len(self._buf) - pos > MAX_INLINE_LEN:
                raise ProtocolError("line too long")
            raise _Incomplete
        return bytes(self._buf[pos:end]), end + 2

    def _parse_int(self, raw: bytes, what: str) -> int:
        try:
            return int(raw)
        except ValueError:
            raise ProtocolError(f"invalid {what} length")

    def _parse_command(self, pos: int) -> tuple[list[bytes], int]:
        if self._buf[pos:pos + 1] != b"*":
            line, pos = self._read_line(pos)          # inline command
            return line.split(), pos

        line, pos = self._read_line(pos)
        count = self._parse_int(line[1:], "multibulk")
        if count > MAX_ARRAY_LEN:
            raise ProtocolError("invalid multibulk length")
        args = []
        for _ in range(max(count, 0)):
            if pos >= len(self._buf):
                raise _Incomplete
            if self._buf[pos:pos + 1] != b"$":
                raise ProtocolError(f"expected '$', got '{chr(self._buf[pos])}'")
            line, pos = self._read_line(pos)
            length = self._parse_int(line[1:], "bulk")
            if not 0 <= length <= MAX_BULK_LEN:
                raise ProtocolError("invalid bulk length")
            if len(self._buf) < pos + length + 2:
                raise _Incomplete
            args.append(bytes(self._buf[pos:pos + length]))
            if self._buf[pos + length:pos + length + 2] != b"\r\n":
                raise ProtocolError("bulk string not terminated by CRLF")
            pos += length + 2
        return args, pos


def parse_reply(buf: bytes, pos: int = 0):
    """Parse one server reply (used by the test client). Returns (value, new_pos).
    Raises IndexError/ValueError if the buffer is incomplete."""
    kind = buf[pos:pos + 1]
    end = buf.index(b"\r\n", pos)
    line = buf[pos + 1:end]
    pos = end + 2
    if kind == b"+":
        return SimpleString(line.decode()), pos
    if kind == b"-":
        return Error(line.decode()), pos
    if kind == b":":
        return int(line), pos
    if kind == b"$":
        n = int(line)
        if n == -1:
            return None, pos
        if len(buf) < pos + n + 2:
            raise IndexError("incomplete bulk")
        return buf[pos:pos + n], pos + n + 2
    if kind == b"*":
        n = int(line)
        if n == -1:
            return NULL_ARRAY, pos
        items = []
        for _ in range(n):
            item, pos = parse_reply(buf, pos)
            items.append(item)
        return items, pos
    raise ValueError(f"unknown reply type {kind!r}")
