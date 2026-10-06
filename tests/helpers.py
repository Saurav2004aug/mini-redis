"""Test helpers: run a MiniRedisServer in a background thread, plus a tiny
synchronous RESP client (so tests need no redis-py)."""
import asyncio
import os
import socket
import sys
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from miniredis.resp import encode_command, parse_reply  # noqa: E402
from miniredis.server import MiniRedisServer  # noqa: E402
from miniredis.store import Store  # noqa: E402


class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000):
        self.ms = start_ms

    def __call__(self) -> int:
        return self.ms


class ServerThread:
    def __init__(self, aof_path=None, fsync="everysec", clock=None):
        self.clock = clock or FakeClock()
        self.server = MiniRedisServer("127.0.0.1", 0, aof_path, fsync, store=Store(clock_ms=self.clock))
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.port = asyncio.run_coroutine_threadsafe(self.server.start(), self.loop).result(5)

    def call_in_loop(self, fn, *args):
        """Run fn on the server's event loop thread (avoids racing with it)."""
        async def wrapper():
            return fn(*args)
        return asyncio.run_coroutine_threadsafe(wrapper(), self.loop).result(5)

    def stop(self):
        asyncio.run_coroutine_threadsafe(self.server.stop(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


class Client:
    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = b""

    def send(self, *args):
        self.sock.sendall(encode_command(*args))

    def read(self):
        while True:
            try:
                value, pos = parse_reply(self.buf)
                self.buf = self.buf[pos:]
                return value
            except (IndexError, ValueError):
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise ConnectionError("server closed connection")
                self.buf += chunk

    def call(self, *args):
        self.send(*args)
        return self.read()

    def close(self):
        self.sock.close()
