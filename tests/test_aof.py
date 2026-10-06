"""Persistence tests: data survives restarts, TTLs stay absolute, crash
recovery handles a torn write, and rewrite compacts the log."""
import os
import tempfile
import threading
import time
import unittest

from helpers import Client, FakeClock, ServerThread
from miniredis.aof import AOF
from miniredis.store import Store


class AofCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "appendonly.aof")
        self.clock = FakeClock()
        self.running = []

    def tearDown(self):
        for st, c in self.running:
            c.close()
            st.stop()
        self.tmp.cleanup()

    def start(self, fsync="always"):
        st = ServerThread(self.path, fsync, clock=self.clock)
        c = Client(st.port)
        self.running.append((st, c))
        return st, c

    def restart(self, fsync="always"):
        st, c = self.running.pop()
        c.close()
        st.stop()
        return self.start(fsync)


class TestAof(AofCase):
    def test_all_types_survive_restart(self):
        _, c = self.start()
        c.call("SET", "s", "hello")
        c.call("INCRBY", "n", "41")
        c.call("INCR", "n")
        c.call("RPUSH", "l", "a", "b", "c")
        c.call("LPOP", "l")
        c.call("HSET", "h", "f1", "v1", "f2", "v2")
        c.call("SADD", "z", "x", "y")
        c.call("DEL", "s")
        c.call("MULTI")
        c.call("SET", "tx", "committed")
        c.call("EXEC")

        _, c = self.restart()
        self.assertIsNone(c.call("GET", "s"))
        self.assertEqual(c.call("GET", "n"), b"42")
        self.assertEqual(c.call("LRANGE", "l", "0", "-1"), [b"b", b"c"])
        self.assertEqual(c.call("HGET", "h", "f2"), b"v2")
        self.assertEqual(c.call("SMEMBERS", "z"), [b"x", b"y"])
        self.assertEqual(c.call("GET", "tx"), b"committed")

    def test_reads_are_not_logged(self):
        _, c = self.start()
        c.call("SET", "k", "v")
        size = os.path.getsize(self.path)
        for _ in range(50):
            c.call("GET", "k")
            c.call("EXISTS", "k")
        self.assertEqual(os.path.getsize(self.path), size)

    def test_relative_ttl_logged_as_absolute(self):
        """If EXPIRE k 10 were logged literally, every restart would reset the
        countdown and the key would never expire."""
        _, c = self.start()
        c.call("SET", "session", "abc", "EX", "10")
        c.call("SET", "other", "x")
        c.call("EXPIRE", "other", "20")
        with open(self.path, "rb") as f:
            log = f.read()
        self.assertIn(b"PXAT", log)
        self.assertIn(b"PEXPIREAT", log)
        self.assertNotIn(b"EXPIRE\r\n$5\r\nother\r\n$2\r\n20", log)

        self.clock.ms += 15_000   # 15s pass while the server is down
        _, c = self.restart()
        self.assertIsNone(c.call("GET", "session"))
        self.assertEqual(c.call("TTL", "other"), 5)

    def test_truncated_tail_is_discarded(self):
        _, c = self.start()
        c.call("SET", "a", "1")
        c.call("SET", "b", "2")
        st, c = self.running.pop()
        c.close()
        st.stop()
        with open(self.path, "ab") as f:
            f.write(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$5\r\nhal")   # simulated crash mid-write
        _, c = self.start()
        self.assertEqual(c.call("MGET", "a", "b", "c"), [b"1", b"2", None])
        c.call("SET", "d", "4")                                  # log is usable again
        _, c = self.restart()
        self.assertEqual(c.call("GET", "d"), b"4")

    def test_rewrite_compacts_and_preserves_data(self):
        _, c = self.start()
        for i in range(500):
            c.call("INCR", "counter")
        c.call("RPUSH", "l", "a", "b")
        c.call("SET", "t", "v", "EX", "100")
        c.call("SET", "gone", "x")
        c.call("DEL", "gone")
        before = os.path.getsize(self.path)
        reply = c.call("BGREWRITEAOF")
        after = os.path.getsize(self.path)
        self.assertIn("rewritten", reply)
        self.assertLess(after, before / 10)

        c.call("INCR", "counter")  # appends after a rewrite still work
        _, c = self.restart()
        self.assertEqual(c.call("GET", "counter"), b"501")
        self.assertEqual(c.call("LRANGE", "l", "0", "-1"), [b"a", b"b"])
        self.assertEqual(c.call("TTL", "t"), 100)
        self.assertEqual(c.call("EXISTS", "gone"), 0)

    def test_everysec_policy_flushes_on_clean_shutdown(self):
        _, c = self.start(fsync="everysec")
        c.call("SET", "k", "v")
        _, c = self.restart(fsync="everysec")
        self.assertEqual(c.call("GET", "k"), b"v")


class TestFsyncPolicies(AofCase):
    def test_fsync_no_still_reaches_the_os_within_about_a_second(self):
        """With fsync=no, writes must still leave Python's buffer regularly,
        otherwise a kill -9 would lose data no matter how old it was."""
        _, c = self.start(fsync="no")
        c.call("SET", "k", "v")
        self.assertEqual(os.path.getsize(self.path), 0)   # still buffered in-process
        deadline = time.monotonic() + 3
        while os.path.getsize(self.path) == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertGreater(os.path.getsize(self.path), 0)

    def test_concurrent_flush_append_and_rewrite(self):
        """Background flushes racing with appends and rewrites must not crash
        or lose writes."""
        aof = AOF(self.path, "everysec")
        aof.open()
        store = Store()
        errors = []
        stop = threading.Event()

        def flusher():
            while not stop.is_set():
                try:
                    aof.flush()
                except Exception as e:  # pragma: no cover - the test fails if hit
                    errors.append(e)

        t = threading.Thread(target=flusher)
        t.start()
        for i in range(3000):
            key = b"k%d" % (i % 50)
            store.set(key, b"%d" % i)
            aof.append([b"SET", key, b"%d" % i])
            if i % 500 == 0:
                aof.rewrite(store)
        stop.set()
        t.join()
        aof.close()
        self.assertEqual(errors, [])

        replayed = Store()
        AOF(self.path).load(lambda args: replayed.set(args[1], args[2]) if args[0] == b"SET" else None)
        self.assertEqual({k: replayed.data[k] for k in replayed.data}, store.data)


class TestWithoutAof(unittest.TestCase):
    def test_bgrewriteaof_without_aof_errors(self):
        st = ServerThread()
        c = Client(st.port)
        try:
            self.assertIn("AOF is not enabled", c.call("BGREWRITEAOF").message)
        finally:
            c.close()
            st.stop()


if __name__ == "__main__":
    unittest.main()
