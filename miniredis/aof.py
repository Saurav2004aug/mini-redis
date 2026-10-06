"""
Append-Only File (AOF) persistence, the same approach Redis uses.

Every successful write command is appended to a log file in RESP format
(exactly the bytes a client would send). On startup the file is replayed
through the normal command path to rebuild the dataset.

fsync policies (like Redis's `appendfsync`):
  always   - fsync after every write. Safest and slowest.
  everysec - fsync once a second from a background task. At most ~1s of
             writes can be lost on power failure. This is the default.
  no       - hand writes to the OS once a second but never fsync; the OS
             decides when they reach the disk

Thread safety: the event loop appends while a background thread flushes, and
BGREWRITEAOF swaps the file. A lock guards the file object. The slow fsync
itself runs on a duplicated file descriptor outside the lock, so appends
never wait on the disk.

Crash safety: if the process dies mid-write, the file can end with a
truncated command. On load, the incomplete tail is discarded and the file is
truncated back to the last complete command, as Redis does with
`aof-load-truncated yes`.

Compaction: `rewrite()` writes the smallest set of commands that recreates the
current dataset to a temp file, then atomically renames it over the old log
(like BGREWRITEAOF).
"""

import os
import threading
from collections import deque

from .resp import RespParser, encode_command
from .store import Store


class AOF:
    def __init__(self, path: str, fsync: str = "everysec"):
        if fsync not in ("always", "everysec", "no"):
            raise ValueError("fsync must be always, everysec or no")
        self.path = path
        self.fsync_policy = fsync
        self._file = None
        self._dirty = False
        self._lock = threading.Lock()

    def load(self, execute) -> int:
        """Replay the log, calling execute(args) per command. Returns the count."""
        if not os.path.exists(self.path):
            return 0
        parser = RespParser()
        count = 0
        valid_bytes = 0
        with open(self.path, "rb") as f:
            data = f.read()
        for command in parser.feed(data):
            execute(command)
            count += 1
        valid_bytes = len(data) - parser.pending_bytes
        if parser.pending_bytes:
            print(f"[aof] discarding {parser.pending_bytes} bytes of truncated command at end of file")
            with open(self.path, "r+b") as f:
                f.truncate(valid_bytes)
        return count

    def open(self):
        with self._lock:
            self._file = open(self.path, "ab")

    def append(self, args: list[bytes]) -> None:
        with self._lock:
            self._file.write(encode_command(*args))
            if self.fsync_policy == "always":
                self._file.flush()
                os.fsync(self._file.fileno())
            else:
                self._dirty = True

    def flush(self) -> None:
        """Called once a second from a background thread (policies everysec/no)."""
        with self._lock:
            if self._file is None or not self._dirty:
                return
            self._dirty = False          # cleared *before* flushing: a concurrent
            self._file.flush()           # append re-marks it and isn't lost
            if self.fsync_policy != "everysec":
                return
            fd = os.dup(self._file.fileno())  # survives a concurrent rewrite() closing the file
        try:
            os.fsync(fd)                 # slow part, outside the lock
        finally:
            os.close(fd)

    def rewrite(self, store: Store) -> int:
        """Compact the log to one command per key. Returns the new file size."""
        tmp = self.path + ".rewrite.tmp"
        with open(tmp, "wb") as f:
            for key in store.keys():
                value = store.data[key]
                if isinstance(value, bytes):
                    f.write(encode_command(b"SET", key, value))
                elif isinstance(value, deque):
                    f.write(encode_command(b"RPUSH", key, *value))
                elif isinstance(value, dict):
                    f.write(encode_command(b"HSET", key, *[x for kv in value.items() for x in kv]))
                elif isinstance(value, set):
                    f.write(encode_command(b"SADD", key, *sorted(value)))
                if key in store.expires:
                    f.write(encode_command(b"PEXPIREAT", key, store.expires[key]))
            f.flush()
            os.fsync(f.fileno())
        with self._lock:
            if self._file:
                self._file.close()
            os.replace(tmp, self.path)  # atomic on POSIX: readers see old or new, never half
            self._file = open(self.path, "ab")
            self._dirty = False
        return os.path.getsize(self.path)

    def close(self):
        self.flush()
        with self._lock:
            if self._file:
                self._file.flush()
                os.fsync(self._file.fileno())
                self._file.close()
                self._file = None
