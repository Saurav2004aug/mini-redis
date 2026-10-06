import unittest

import helpers  # noqa: F401  (sets up sys.path)
from miniredis.resp import (NULL_ARRAY, OK, Error, ProtocolError, RespParser,
                            SimpleString, encode, encode_command, parse_reply)


class TestEncode(unittest.TestCase):
    def test_all_types(self):
        cases = [
            (OK, b"+OK\r\n"),
            (Error("ERR boom"), b"-ERR boom\r\n"),
            (42, b":42\r\n"),
            (-1, b":-1\r\n"),
            (b"hello", b"$5\r\nhello\r\n"),
            (b"", b"$0\r\n\r\n"),
            (None, b"$-1\r\n"),
            (NULL_ARRAY, b"*-1\r\n"),
            ([], b"*0\r\n"),
            ([b"a", 1, None], b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"),
            ([[b"x"], OK], b"*2\r\n*1\r\n$1\r\nx\r\n+OK\r\n"),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(encode(value), expected)

    def test_binary_safe(self):
        blob = bytes(range(256)) + b"\r\n\x00"
        self.assertEqual(parse_reply(encode(blob))[0], blob)

    def test_roundtrip_through_parse_reply(self):
        value = [b"a", 7, [b"nested", None], SimpleString("PONG")]
        decoded, pos = parse_reply(encode(value))
        self.assertEqual(decoded, value)


class TestParser(unittest.TestCase):
    def test_single_command(self):
        self.assertEqual(list(RespParser().feed(encode_command("SET", "k", "v"))), [[b"SET", b"k", b"v"]])

    def test_byte_by_byte_delivery(self):
        """TCP can split a command anywhere, even inside '\\r\\n'."""
        data = encode_command("SET", "key", "value with spaces\r\nand CRLF")
        p = RespParser()
        out = []
        for i in range(len(data)):
            out += p.feed(data[i:i + 1])
        self.assertEqual(out, [[b"SET", b"key", b"value with spaces\r\nand CRLF"]])
        self.assertEqual(p.pending_bytes, 0)

    def test_pipelined_commands_in_one_read(self):
        data = b"".join(encode_command("INCR", "c") for _ in range(100))
        self.assertEqual(len(list(RespParser().feed(data))), 100)

    def test_inline_commands(self):
        p = RespParser()
        self.assertEqual(list(p.feed(b"PING\r\nSET a  b\r\n\r\n")), [[b"PING"], [b"SET", b"a", b"b"]])

    def test_protocol_errors(self):
        for bad in (b"*1\r\n:5\r\n", b"*x\r\n", b"*1\r\n$-5\r\n", b"*1\r\n$3\r\nabcXY"):
            with self.subTest(bad=bad), self.assertRaises(ProtocolError):
                list(RespParser().feed(bad))

    def test_line_too_long(self):
        with self.assertRaises(ProtocolError):
            list(RespParser().feed(b"x" * 70_000))


if __name__ == "__main__":
    unittest.main()
