"""Integration tests: real TCP server, real RESP over the wire."""
import unittest

from helpers import Client, ServerThread
from miniredis.resp import NULL_ARRAY, Error, encode_command


class ServerCase(unittest.TestCase):
    def setUp(self):
        self.st = ServerThread()
        self.clock = self.st.clock
        self.c = Client(self.st.port)
        self.extra = []

    def tearDown(self):
        for cl in [self.c, *self.extra]:
            cl.close()
        self.st.stop()

    def new_client(self):
        cl = Client(self.st.port)
        self.extra.append(cl)
        return cl

    def assertError(self, reply, prefix):
        self.assertIsInstance(reply, Error, reply)
        self.assertTrue(reply.message.startswith(prefix), reply.message)


class TestBasics(ServerCase):
    def test_ping_echo(self):
        self.assertEqual(self.c.call("PING"), "PONG")
        self.assertEqual(self.c.call("PING", "hi"), b"hi")
        self.assertEqual(self.c.call("ECHO", "hello"), b"hello")

    def test_commands_are_case_insensitive(self):
        self.assertEqual(self.c.call("set", "k", "v"), "OK")
        self.assertEqual(self.c.call("gEt", "k"), b"v")

    def test_unknown_command_and_arity(self):
        self.assertError(self.c.call("NOPE"), "ERR unknown command")
        self.assertError(self.c.call("GET"), "ERR wrong number of arguments for 'get'")
        self.assertError(self.c.call("GET", "a", "b"), "ERR wrong number of arguments")

    def test_pipelining_1000_commands_in_one_write(self):
        self.c.sock.sendall(b"".join(encode_command("INCR", "n") for _ in range(1000)))
        replies = [self.c.read() for _ in range(1000)]
        self.assertEqual(replies, list(range(1, 1001)))

    def test_protocol_error_closes_connection(self):
        self.c.sock.sendall(b"*1\r\n:oops\r\n")
        self.assertError(self.c.read(), "ERR Protocol error")
        with self.assertRaises(ConnectionError):
            self.c.read()

    def test_info_and_dbsize(self):
        self.c.call("MSET", "a", "1", "b", "2")
        self.assertEqual(self.c.call("DBSIZE"), 2)
        info = self.c.call("INFO").decode()
        self.assertIn("connected_clients:1", info)
        self.assertIn("db0:keys=2,expires=0", info)


class TestStrings(ServerCase):
    def test_set_get_overwrite(self):
        self.assertIsNone(self.c.call("GET", "k"))
        self.c.call("SET", "k", "v1")
        self.c.call("SET", "k", "v2")
        self.assertEqual(self.c.call("GET", "k"), b"v2")

    def test_set_nx_xx_get(self):
        self.assertIsNone(self.c.call("SET", "k", "v", "XX"))
        self.assertEqual(self.c.call("SET", "k", "v", "NX"), "OK")
        self.assertIsNone(self.c.call("SET", "k", "other", "NX"))
        self.assertEqual(self.c.call("SET", "k", "new", "GET"), b"v")
        self.assertEqual(self.c.call("GET", "k"), b"new")
        self.assertError(self.c.call("SET", "k", "v", "NX", "XX"), "ERR syntax error")
        self.assertError(self.c.call("SET", "k", "v", "EX", "0"), "ERR invalid expire time")

    def test_set_with_ttl_options(self):
        self.c.call("SET", "a", "1", "EX", "10")
        self.c.call("SET", "b", "1", "PX", "1500")
        self.assertEqual(self.c.call("TTL", "a"), 10)
        self.assertEqual(self.c.call("PTTL", "b"), 1500)
        self.clock.ms += 1500
        self.assertIsNone(self.c.call("GET", "b"))
        self.assertEqual(self.c.call("GET", "a"), b"1")

    def test_keepttl(self):
        self.c.call("SET", "k", "1", "EX", "100")
        self.c.call("SET", "k", "2", "KEEPTTL")
        self.assertEqual(self.c.call("TTL", "k"), 100)

    def test_incr_family(self):
        self.assertEqual(self.c.call("INCR", "n"), 1)
        self.assertEqual(self.c.call("INCRBY", "n", "10"), 11)
        self.assertEqual(self.c.call("DECR", "n"), 10)
        self.assertEqual(self.c.call("DECRBY", "n", "20"), -10)
        self.c.call("SET", "s", "abc")
        self.assertError(self.c.call("INCR", "s"), "ERR value is not an integer")
        self.c.call("SET", "big", str(2 ** 63 - 1))
        self.assertError(self.c.call("INCR", "big"), "ERR increment or decrement would overflow")

    def test_incr_preserves_ttl(self):
        self.c.call("SET", "n", "1", "EX", "50")
        self.c.call("INCR", "n")
        self.assertEqual(self.c.call("TTL", "n"), 50)

    def test_append_strlen_mget_setnx_getdel(self):
        self.assertEqual(self.c.call("APPEND", "s", "Hello"), 5)
        self.assertEqual(self.c.call("APPEND", "s", " World"), 11)
        self.assertEqual(self.c.call("STRLEN", "s"), 11)
        self.c.call("RPUSH", "list", "x")
        self.assertEqual(self.c.call("MGET", "s", "missing", "list"), [b"Hello World", None, None])
        self.assertEqual(self.c.call("SETNX", "s", "x"), 0)
        self.assertEqual(self.c.call("GETDEL", "s"), b"Hello World")
        self.assertEqual(self.c.call("EXISTS", "s"), 0)


class TestKeys(ServerCase):
    def test_del_exists_counts(self):
        self.c.call("MSET", "a", "1", "b", "2")
        self.assertEqual(self.c.call("EXISTS", "a", "b", "c", "a"), 3)
        self.assertEqual(self.c.call("DEL", "a", "b", "c"), 2)

    def test_keys_glob(self):
        self.c.call("MSET", "user:1", "a", "user:2", "b", "session:1", "c")
        self.assertEqual(sorted(self.c.call("KEYS", "user:*")), [b"user:1", b"user:2"])
        self.assertEqual(len(self.c.call("KEYS", "*")), 3)
        self.assertEqual(self.c.call("KEYS", "user:?"), self.c.call("KEYS", "user:[12]"))

    def test_type(self):
        self.c.call("SET", "s", "x")
        self.c.call("RPUSH", "l", "x")
        self.c.call("HSET", "h", "f", "v")
        self.c.call("SADD", "z", "m")
        self.assertEqual([self.c.call("TYPE", k) for k in ("s", "l", "h", "z", "nope")],
                         ["string", "list", "hash", "set", "none"])

    def test_expire_ttl_persist(self):
        self.assertEqual(self.c.call("EXPIRE", "missing", "10"), 0)
        self.c.call("SET", "k", "v")
        self.assertEqual(self.c.call("TTL", "k"), -1)
        self.assertEqual(self.c.call("EXPIRE", "k", "10"), 1)
        self.clock.ms += 4001
        self.assertEqual(self.c.call("TTL", "k"), 6)  # rounds up like Redis
        self.assertEqual(self.c.call("PERSIST", "k"), 1)
        self.assertEqual(self.c.call("TTL", "k"), -1)
        self.assertEqual(self.c.call("TTL", "missing"), -2)

    def test_expireat_in_the_past_deletes(self):
        self.c.call("SET", "k", "v")
        self.c.call("EXPIREAT", "k", "1")
        self.assertEqual(self.c.call("EXISTS", "k"), 0)

    def test_rename_keeps_ttl(self):
        self.c.call("SET", "a", "v", "EX", "30")
        self.assertEqual(self.c.call("RENAME", "a", "b"), "OK")
        self.assertEqual(self.c.call("TTL", "b"), 30)
        self.assertError(self.c.call("RENAME", "nope", "x"), "ERR no such key")


class TestCollections(ServerCase):
    def test_lists(self):
        self.assertEqual(self.c.call("RPUSH", "l", "a", "b", "c"), 3)
        self.assertEqual(self.c.call("LPUSH", "l", "z", "y"), 5)       # y z a b c
        self.assertEqual(self.c.call("LRANGE", "l", "0", "-1"), [b"y", b"z", b"a", b"b", b"c"])
        self.assertEqual(self.c.call("LRANGE", "l", "-2", "100"), [b"b", b"c"])
        self.assertEqual(self.c.call("LRANGE", "l", "3", "1"), [])
        self.assertEqual(self.c.call("LINDEX", "l", "-1"), b"c")
        self.assertEqual(self.c.call("LPOP", "l"), b"y")
        self.assertEqual(self.c.call("RPOP", "l", "2"), [b"c", b"b"])
        self.assertEqual(self.c.call("LLEN", "l"), 2)
        self.assertEqual(self.c.call("LPOP", "l", "0"), [])           # count 0 -> empty array
        self.assertEqual(self.c.call("LLEN", "l"), 2)
        self.c.call("RPOP", "l", "10")
        self.assertEqual(self.c.call("EXISTS", "l"), 0)                # empty list removed
        self.assertIsNone(self.c.call("LPOP", "l"))
        self.assertEqual(self.c.call("LPOP", "l", "1"), NULL_ARRAY)

    def test_hashes(self):
        self.assertEqual(self.c.call("HSET", "u", "name", "Aman", "city", "Delhi"), 2)
        self.assertEqual(self.c.call("HSET", "u", "city", "Mumbai"), 0)  # update, not add
        self.assertEqual(self.c.call("HGET", "u", "city"), b"Mumbai")
        self.assertEqual(self.c.call("HMGET", "u", "name", "nope"), [b"Aman", None])
        self.assertEqual(self.c.call("HLEN", "u"), 2)
        self.assertEqual(self.c.call("HEXISTS", "u", "name"), 1)
        self.assertEqual(self.c.call("HINCRBY", "u", "visits", "5"), 5)
        all_ = self.c.call("HGETALL", "u")
        self.assertEqual(dict(zip(all_[::2], all_[1::2]))[b"visits"], b"5")
        self.assertEqual(self.c.call("HDEL", "u", "name", "city", "visits", "x"), 3)
        self.assertEqual(self.c.call("EXISTS", "u"), 0)

    def test_sets(self):
        self.assertEqual(self.c.call("SADD", "a", "1", "2", "3", "3"), 3)
        self.c.call("SADD", "b", "2", "3", "4")
        self.assertEqual(self.c.call("SISMEMBER", "a", "2"), 1)
        self.assertEqual(self.c.call("SCARD", "a"), 3)
        self.assertEqual(self.c.call("SINTER", "a", "b"), [b"2", b"3"])
        self.assertEqual(self.c.call("SUNION", "a", "b"), [b"1", b"2", b"3", b"4"])
        self.assertEqual(self.c.call("SREM", "a", "1", "9"), 1)
        self.assertEqual(self.c.call("SMEMBERS", "a"), [b"2", b"3"])

    def test_wrongtype_errors(self):
        self.c.call("SET", "s", "x")
        for cmd in (("LPUSH", "s", "a"), ("HGET", "s", "f"), ("SADD", "s", "m"), ("LRANGE", "s", "0", "1")):
            with self.subTest(cmd=cmd):
                self.assertError(self.c.call(*cmd), "WRONGTYPE")
        self.c.call("RPUSH", "l", "a")
        self.assertError(self.c.call("GET", "l"), "WRONGTYPE")


class TestTransactions(ServerCase):
    def test_multi_exec(self):
        self.assertEqual(self.c.call("MULTI"), "OK")
        self.assertEqual(self.c.call("SET", "a", "1"), "QUEUED")
        self.assertEqual(self.c.call("INCR", "a"), "QUEUED")
        self.assertEqual(self.c.call("GET", "a"), "QUEUED")
        self.assertEqual(self.c.call("EXEC"), ["OK", 2, b"2"])

    def test_runtime_error_does_not_abort_other_commands(self):
        self.c.call("SET", "s", "text")
        self.c.call("MULTI")
        self.c.call("INCR", "s")
        self.c.call("SET", "t", "ok")
        result = self.c.call("EXEC")
        self.assertIsInstance(result[0], Error)
        self.assertEqual(result[1], "OK")   # same as Redis: no rollback

    def test_queue_time_error_aborts_whole_transaction(self):
        self.c.call("MULTI")
        self.c.call("SET", "a", "1")
        self.assertError(self.c.call("GET"), "ERR wrong number")
        self.assertError(self.c.call("EXEC"), "EXECABORT")
        self.assertIsNone(self.c.call("GET", "a"))

    def test_subscribe_not_allowed_in_transaction(self):
        self.c.call("MULTI")
        self.assertError(self.c.call("SUBSCRIBE", "ch"), "ERR Command SUBSCRIBE is not allowed")
        self.assertError(self.c.call("EXEC"), "EXECABORT")
        self.assertEqual(self.c.call("PING"), "PONG")  # connection still healthy

    def test_discard_and_misuse(self):
        self.c.call("MULTI")
        self.c.call("SET", "a", "1")
        self.assertEqual(self.c.call("DISCARD"), "OK")
        self.assertIsNone(self.c.call("GET", "a"))
        self.assertError(self.c.call("EXEC"), "ERR EXEC without MULTI")
        self.c.call("MULTI")
        self.assertError(self.c.call("MULTI"), "ERR MULTI calls can not be nested")

    def test_other_clients_never_see_partial_transaction(self):
        other = self.new_client()
        self.c.call("MULTI")
        self.c.call("SET", "x", "1")
        self.assertIsNone(other.call("GET", "x"))  # queued, not applied yet
        self.c.call("EXEC")
        self.assertEqual(other.call("GET", "x"), b"1")


class TestPubSub(ServerCase):
    def test_publish_subscribe(self):
        sub = self.new_client()
        self.assertEqual(sub.call("SUBSCRIBE", "news", "sports"), [b"subscribe", b"news", 1])
        self.assertEqual(sub.read(), [b"subscribe", b"sports", 2])
        self.assertEqual(self.c.call("PUBLISH", "news", "hello"), 1)
        self.assertEqual(sub.read(), [b"message", b"news", b"hello"])
        self.assertEqual(self.c.call("PUBLISH", "nobody-listening", "x"), 0)

    def test_subscribed_client_restricted_commands(self):
        sub = self.new_client()
        sub.call("SUBSCRIBE", "ch")
        self.assertError(sub.call("GET", "k"), "ERR Can't execute 'get'")
        self.assertEqual(sub.call("PING"), [b"pong", b""])

    def test_unsubscribe_and_disconnect_cleanup(self):
        sub = self.new_client()
        sub.call("SUBSCRIBE", "a")
        self.assertEqual(sub.call("UNSUBSCRIBE", "a"), [b"unsubscribe", b"a", 0])
        self.assertEqual(sub.call("GET", "k"), None)     # back to normal mode
        sub.call("SUBSCRIBE", "b")
        sub.close()
        self.extra.remove(sub)
        # After disconnect, nobody is subscribed to 'b' any more
        for _ in range(50):
            if self.c.call("PUBLISH", "b", "x") == 0:
                break
        self.assertEqual(self.c.call("PUBLISH", "b", "x"), 0)


if __name__ == "__main__":
    unittest.main()
