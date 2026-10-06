"""
Command implementations and the dispatch table.

Each handler is registered with Redis-style metadata:
  arity   > 0: exact number of args including the command name
  arity   < 0: at least |arity| args
  write:  the command mutates data, so it is appended to the AOF

Handlers receive (ctx, args), where args excludes the command name, and return
a RESP reply or raise resp.Error. A handler can call ctx.propagate(...) to log
a different, deterministic form of itself to the AOF. For example, relative
TTLs are logged as absolute PEXPIREAT, so replaying the file later doesn't
extend them.
"""

import fnmatch
from collections import deque
from dataclasses import dataclass
from typing import Callable

from . import resp
from .resp import OK, PONG, Error, SimpleString

COMMANDS: dict[bytes, "Command"] = {}


@dataclass
class Command:
    name: str
    handler: Callable
    arity: int
    write: bool
    pubsub_ok: bool = False  # allowed while the client is in subscribe mode


def command(name: str, arity: int, write: bool = False, pubsub_ok: bool = False):
    def register(fn):
        COMMANDS[name.encode()] = Command(name, fn, arity, write, pubsub_ok)
        return fn
    return register


# ---------- argument helpers ----------

def to_int(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise Error("ERR value is not an integer or out of range")


def syntax_error():
    return Error("ERR syntax error")


# =====================================================================
# Connection / server
# =====================================================================

@command("PING", -1, pubsub_ok=True)
def ping(ctx, args):
    if ctx.client.subscriptions:
        return [b"pong", args[0] if args else b""]
    return args[0] if args else PONG


@command("ECHO", 2)
def echo(ctx, args):
    return args[0]


@command("SELECT", 2)
def select(ctx, args):
    if args[0] != b"0":
        raise Error("ERR DB index is out of range (mini-redis has a single database)")
    return OK


@command("QUIT", 1, pubsub_ok=True)
def quit_(ctx, args):
    ctx.client.closing = True
    return OK


@command("CLIENT", -2)
def client_cmd(ctx, args):
    sub = args[0].upper()
    if sub == b"SETNAME" and len(args) == 2:
        ctx.client.name = args[1]
        return OK
    if sub == b"GETNAME":
        return ctx.client.name
    if sub == b"ID":
        return ctx.client.id
    if sub == b"SETINFO":  # sent by redis-py and other modern clients on connect
        return OK
    raise Error(f"ERR unknown subcommand '{args[0].decode(errors='replace')}'")


@command("COMMAND", -1)
def command_cmd(ctx, args):
    if args and args[0].upper() == b"COUNT":
        return len(COMMANDS)
    return []  # redis-cli asks for COMMAND DOCS on start; an empty list is accepted


@command("CONFIG", -2)
def config_cmd(ctx, args):
    if args[0].upper() == b"GET":
        return []
    return OK


@command("DBSIZE", 1)
def dbsize(ctx, args):
    return len(ctx.store.keys())


@command("FLUSHDB", -1, write=True)
def flushdb(ctx, args):
    ctx.store.flush()
    return OK


COMMANDS[b"FLUSHALL"] = Command("FLUSHALL", flushdb, -1, True)


@command("INFO", -1)
def info(ctx, args):
    return ctx.server.info().encode()


# =====================================================================
# Keys
# =====================================================================

@command("DEL", -2, write=True)
def delete(ctx, args):
    return sum(ctx.store.delete(k) for k in args if ctx.store.exists(k))


COMMANDS[b"UNLINK"] = Command("UNLINK", delete, -2, True)


@command("EXISTS", -2)
def exists(ctx, args):
    return sum(ctx.store.exists(k) for k in args)


@command("TYPE", 2)
def type_cmd(ctx, args):
    return SimpleString(ctx.store.type_name(args[0]))


@command("KEYS", 2)
def keys(ctx, args):
    pattern = args[0].decode(errors="surrogateescape")
    return [k for k in ctx.store.keys()
            if fnmatch.fnmatchcase(k.decode(errors="surrogateescape"), pattern)]


@command("RENAME", 3, write=True)
def rename(ctx, args):
    src, dst = args
    if not ctx.store.exists(src):
        raise Error("ERR no such key")
    deadline = ctx.store.expires.get(src)
    value = ctx.store.data[src]
    ctx.store.delete(src)
    ctx.store.set(dst, value)
    if deadline is not None:
        ctx.store.expires[dst] = deadline
    return OK


def _expire_generic(ctx, key: bytes, deadline_ms: int) -> int:
    ok = ctx.store.set_expiry(key, deadline_ms)
    if ok:
        ctx.propagate(b"PEXPIREAT", key, str(deadline_ms).encode())
    else:
        ctx.propagate()  # nothing to log
    return int(ok)


@command("EXPIRE", 3, write=True)
def expire(ctx, args):
    return _expire_generic(ctx, args[0], ctx.store.clock_ms() + to_int(args[1]) * 1000)


@command("PEXPIRE", 3, write=True)
def pexpire(ctx, args):
    return _expire_generic(ctx, args[0], ctx.store.clock_ms() + to_int(args[1]))


@command("EXPIREAT", 3, write=True)
def expireat(ctx, args):
    return _expire_generic(ctx, args[0], to_int(args[1]) * 1000)


@command("PEXPIREAT", 3, write=True)
def pexpireat(ctx, args):
    return _expire_generic(ctx, args[0], to_int(args[1]))


@command("TTL", 2)
def ttl(ctx, args):
    ms = ctx.store.ttl_ms(args[0])
    return ms if ms < 0 else (ms + 999) // 1000  # round up, like Redis


@command("PTTL", 2)
def pttl(ctx, args):
    return ctx.store.ttl_ms(args[0])


@command("PERSIST", 2, write=True)
def persist(ctx, args):
    return int(ctx.store.persist(args[0]))


# =====================================================================
# Strings
# =====================================================================

@command("GET", 2)
def get(ctx, args):
    return ctx.store.get(args[0], bytes)


@command("SET", -3, write=True)
def set_(ctx, args):
    """SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]"""
    key, value, opts = args[0], args[1], [a.upper() for a in args[2:]]
    raw_opts = args[2:]
    nx = xx = want_get = keep_ttl = False
    deadline = None
    i = 0
    while i < len(opts):
        opt = opts[i]
        if opt == b"NX" and not xx:
            nx = True
        elif opt == b"XX" and not nx:
            xx = True
        elif opt == b"GET":
            want_get = True
        elif opt == b"KEEPTTL" and deadline is None:
            keep_ttl = True
        elif opt in (b"EX", b"PX", b"EXAT", b"PXAT") and deadline is None and not keep_ttl:
            if i + 1 >= len(opts):
                raise syntax_error()
            n = to_int(raw_opts[i + 1])
            if n <= 0:
                raise Error("ERR invalid expire time in 'set' command")
            now = ctx.store.clock_ms()
            deadline = {b"EX": now + n * 1000, b"PX": now + n,
                        b"EXAT": n * 1000, b"PXAT": n}[opt]
            i += 1
        else:
            raise syntax_error()
        i += 1

    old = ctx.store.get(key, bytes) if want_get else None
    exists = ctx.store.exists(key)
    if (nx and exists) or (xx and not exists):
        ctx.propagate()
        return old if want_get else None

    ctx.store.set(key, value, keep_ttl=keep_ttl)
    if deadline is not None:
        ctx.store.set_expiry(key, deadline)
        ctx.propagate(b"SET", key, value, b"PXAT", str(deadline).encode())
    elif keep_ttl:
        ctx.propagate(b"SET", key, value, b"KEEPTTL")
    else:
        ctx.propagate(b"SET", key, value)
    return old if want_get else OK


@command("SETEX", 4, write=True)
def setex(ctx, args):
    return set_(ctx, [args[0], args[2], b"EX", args[1]])


@command("SETNX", 3, write=True)
def setnx(ctx, args):
    return int(set_(ctx, [args[0], args[1], b"NX"]) is OK)


@command("GETDEL", 2, write=True)
def getdel(ctx, args):
    value = ctx.store.get(args[0], bytes)
    if value is not None:
        ctx.store.delete(args[0])
    ctx.propagate(b"DEL", args[0])
    return value


@command("MGET", -2)
def mget(ctx, args):
    out = []
    for k in args:
        v = ctx.store.get(k)
        out.append(v if isinstance(v, bytes) else None)
    return out


@command("MSET", -3, write=True)
def mset(ctx, args):
    if len(args) % 2:
        raise Error("ERR wrong number of arguments for 'mset' command")
    for k, v in zip(args[::2], args[1::2]):
        ctx.store.set(k, v)
    return OK


def _incr_by(ctx, key: bytes, delta: int) -> int:
    current = ctx.store.get(key, bytes)
    value = to_int(current) if current is not None else 0
    value += delta
    if not -(2 ** 63) <= value < 2 ** 63:
        raise Error("ERR increment or decrement would overflow")
    ctx.store.set(key, str(value).encode(), keep_ttl=True)
    return value


@command("INCR", 2, write=True)
def incr(ctx, args):
    return _incr_by(ctx, args[0], 1)


@command("DECR", 2, write=True)
def decr(ctx, args):
    return _incr_by(ctx, args[0], -1)


@command("INCRBY", 3, write=True)
def incrby(ctx, args):
    return _incr_by(ctx, args[0], to_int(args[1]))


@command("DECRBY", 3, write=True)
def decrby(ctx, args):
    return _incr_by(ctx, args[0], -to_int(args[1]))


@command("APPEND", 3, write=True)
def append(ctx, args):
    value = (ctx.store.get(args[0], bytes) or b"") + args[1]
    ctx.store.set(args[0], value, keep_ttl=True)
    return len(value)


@command("STRLEN", 2)
def strlen(ctx, args):
    return len(ctx.store.get(args[0], bytes) or b"")


# =====================================================================
# Lists
# =====================================================================

@command("LPUSH", -3, write=True)
def lpush(ctx, args):
    lst = ctx.store.get_or_create(args[0], deque)
    lst.extendleft(args[1:])
    return len(lst)


@command("RPUSH", -3, write=True)
def rpush(ctx, args):
    lst = ctx.store.get_or_create(args[0], deque)
    lst.extend(args[1:])
    return len(lst)


def _pop(ctx, args, left: bool):
    lst = ctx.store.get(args[0], deque)
    count = to_int(args[1]) if len(args) > 1 else None
    if count is not None and count < 0:
        raise Error("ERR value is out of range, must be positive")
    if not lst:
        return resp.NULL_ARRAY if count is not None else None
    if count == 0:
        return []  # Redis: LPOP key 0 -> empty array
    pop = lst.popleft if left else lst.pop
    items = [pop() for _ in range(min(count if count is not None else 1, len(lst)))]
    ctx.store.delete_if_empty(args[0])
    return items if count is not None else items[0]


@command("LPOP", -2, write=True)
def lpop(ctx, args):
    return _pop(ctx, args, left=True)


@command("RPOP", -2, write=True)
def rpop(ctx, args):
    return _pop(ctx, args, left=False)


@command("LLEN", 2)
def llen(ctx, args):
    return len(ctx.store.get(args[0], deque) or ())


@command("LRANGE", 4)
def lrange(ctx, args):
    lst = ctx.store.get(args[0], deque) or deque()
    n = len(lst)
    start, stop = to_int(args[1]), to_int(args[2])
    if start < 0:
        start = max(0, n + start)
    if stop < 0:
        stop = n + stop
    stop = min(stop, n - 1)
    if start > stop:
        return []
    return [lst[i] for i in range(start, stop + 1)]


@command("LINDEX", 3)
def lindex(ctx, args):
    lst = ctx.store.get(args[0], deque) or deque()
    i = to_int(args[1])
    try:
        return lst[i]
    except IndexError:
        return None


# =====================================================================
# Hashes
# =====================================================================

@command("HSET", -4, write=True)
def hset(ctx, args):
    if len(args) % 2 == 0:
        raise Error("ERR wrong number of arguments for 'hset' command")
    h = ctx.store.get_or_create(args[0], dict)
    added = 0
    for field, value in zip(args[1::2], args[2::2]):
        added += field not in h
        h[field] = value
    return added


@command("HGET", 3)
def hget(ctx, args):
    return (ctx.store.get(args[0], dict) or {}).get(args[1])


@command("HMGET", -3)
def hmget(ctx, args):
    h = ctx.store.get(args[0], dict) or {}
    return [h.get(f) for f in args[1:]]


@command("HDEL", -3, write=True)
def hdel(ctx, args):
    h = ctx.store.get(args[0], dict)
    if not h:
        return 0
    removed = sum(h.pop(f, None) is not None for f in args[1:])
    ctx.store.delete_if_empty(args[0])
    return removed


@command("HGETALL", 2)
def hgetall(ctx, args):
    h = ctx.store.get(args[0], dict) or {}
    return [x for pair in h.items() for x in pair]


@command("HLEN", 2)
def hlen(ctx, args):
    return len(ctx.store.get(args[0], dict) or {})


@command("HEXISTS", 3)
def hexists(ctx, args):
    return int(args[1] in (ctx.store.get(args[0], dict) or {}))


@command("HINCRBY", 4, write=True)
def hincrby(ctx, args):
    h = ctx.store.get_or_create(args[0], dict)
    current = h.get(args[1], b"0")
    try:
        value = int(current) + to_int(args[2])
    except ValueError:
        raise Error("ERR hash value is not an integer")
    h[args[1]] = str(value).encode()
    return value


# =====================================================================
# Sets
# =====================================================================

@command("SADD", -3, write=True)
def sadd(ctx, args):
    s = ctx.store.get_or_create(args[0], set)
    before = len(s)
    s.update(args[1:])
    return len(s) - before


@command("SREM", -3, write=True)
def srem(ctx, args):
    s = ctx.store.get(args[0], set)
    if not s:
        return 0
    before = len(s)
    s.difference_update(args[1:])
    removed = before - len(s)
    ctx.store.delete_if_empty(args[0])
    return removed


@command("SMEMBERS", 2)
def smembers(ctx, args):
    return sorted(ctx.store.get(args[0], set) or ())


@command("SISMEMBER", 3)
def sismember(ctx, args):
    return int(args[1] in (ctx.store.get(args[0], set) or ()))


@command("SCARD", 2)
def scard(ctx, args):
    return len(ctx.store.get(args[0], set) or ())


@command("SINTER", -2)
def sinter(ctx, args):
    sets = [ctx.store.get(k, set) or set() for k in args]
    return sorted(set.intersection(*sets))


@command("SUNION", -2)
def sunion(ctx, args):
    return sorted(set().union(*(ctx.store.get(k, set) or set() for k in args)))


# =====================================================================
# Pub/Sub
# =====================================================================

@command("SUBSCRIBE", -2, pubsub_ok=True)
def subscribe(ctx, args):
    for channel in args:
        ctx.server.pubsub_subscribe(ctx.client, channel)
        ctx.client.send([b"subscribe", channel, len(ctx.client.subscriptions)])
    return _NO_REPLY


@command("UNSUBSCRIBE", -1, pubsub_ok=True)
def unsubscribe(ctx, args):
    channels = args or sorted(ctx.client.subscriptions)
    if not channels:
        ctx.client.send([b"unsubscribe", None, 0])
    for channel in channels:
        ctx.server.pubsub_unsubscribe(ctx.client, channel)
        ctx.client.send([b"unsubscribe", channel, len(ctx.client.subscriptions)])
    return _NO_REPLY


@command("PUBLISH", 3)
def publish(ctx, args):
    return ctx.server.pubsub_publish(args[0], args[1])


class _NoReply:
    """Sentinel: the handler already sent its own replies."""


_NO_REPLY = _NoReply()
NO_REPLY = _NO_REPLY


# =====================================================================
# Persistence
# =====================================================================

@command("BGREWRITEAOF", 1)
def bgrewriteaof(ctx, args):
    # Note: mini-redis rewrites synchronously. Real Redis forks a child
    # process and uses copy-on-write memory to do this in the background.
    size = ctx.server.rewrite_aof()
    return SimpleString(f"Append only file rewritten ({size} bytes)")
