"""
The asyncio TCP server: connection handling, command dispatch, transactions,
pub/sub, background expiry and AOF flushing.

Like real Redis, all commands run on a single thread (the event loop). This
makes every command, and every MULTI/EXEC block, atomic without any locks.
Concurrency comes from non-blocking I/O, not from threads.
"""

import asyncio
import os
import time
from typing import Optional

from . import __version__
from .aof import AOF
from .commands import COMMANDS, NO_REPLY
from .resp import OK, QUEUED, Error, ProtocolError, RespParser, encode
from .store import Store


class ClientConn:
    _next_id = 1

    def __init__(self, writer: Optional[asyncio.StreamWriter]):
        self.id = ClientConn._next_id
        ClientConn._next_id += 1
        self.writer = writer
        self.name: Optional[bytes] = None
        self.subscriptions: set[bytes] = set()
        self.closing = False
        # MULTI/EXEC state
        self.in_multi = False
        self.multi_queue: list[list[bytes]] = []
        self.multi_error = False

    def send(self, reply) -> None:
        if self.writer is not None and not self.writer.is_closing():
            self.writer.write(encode(reply))


class Context:
    """Passed to every command handler."""
    __slots__ = ("store", "server", "client", "propagated")

    def __init__(self, store, server, client):
        self.store = store
        self.server = server
        self.client = client
        self.propagated: Optional[list[list[bytes]]] = None

    def propagate(self, *args: bytes) -> None:
        """Log `args` to the AOF instead of the original command.
        Called with no args: log nothing (the write was a no-op)."""
        if self.propagated is None:
            self.propagated = []
        if args:
            self.propagated.append(list(args))


class MiniRedisServer:
    def __init__(self, host="127.0.0.1", port=6379, aof_path=None, fsync="everysec",
                 store: Optional[Store] = None):
        self.host = host
        self.port = port
        self.store = store if store is not None else Store()  # not `or`: an empty Store is falsy
        self.aof = AOF(aof_path, fsync) if aof_path else None
        self.channels: dict[bytes, set[ClientConn]] = {}
        self.clients: set[ClientConn] = set()
        self.started_at = time.time()
        self.stats = {"total_connections_received": 0, "total_commands_processed": 0}
        self._server: Optional[asyncio.base_events.Server] = None
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> int:
        if self.aof:
            replay_client = ClientConn(None)
            started = time.perf_counter()
            n = self.aof.load(lambda args: self.execute(replay_client, args, replaying=True))
            if n:
                print(f"[aof] replayed {n} commands in {time.perf_counter() - started:.3f}s")
            self.aof.open()
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._tasks = [asyncio.create_task(self._active_expire_loop())]
        if self.aof and self.aof.fsync_policy != "always":
            self._tasks.append(asyncio.create_task(self._aof_flush_loop()))
        return self.port

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._server:
            self._server.close()
        for c in list(self.clients):
            if c.writer:
                c.writer.close()
        if self._server:
            await self._server.wait_closed()
        if self.aof:
            self.aof.close()

    async def _active_expire_loop(self):
        while True:
            await asyncio.sleep(0.1)  # Redis runs this 10x per second (hz 10)
            self.store.active_expire_cycle()

    async def _aof_flush_loop(self):
        """Once a second: hand buffered writes to the OS, and fsync if
        policy=everysec. fsync can block on slow disks, so it runs in a thread."""
        while True:
            await asyncio.sleep(1)
            try:
                await asyncio.to_thread(self.aof.flush)
            except Exception as e:  # never let one bad flush stop durability for good
                print(f"[aof] background flush failed: {e!r}", flush=True)

    # ------------------------------------------------------------------
    # Networking
    # ------------------------------------------------------------------

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        client = ClientConn(writer)
        self.clients.add(client)
        self.stats["total_connections_received"] += 1
        parser = RespParser()
        try:
            while not client.closing:
                data = await reader.read(65536)
                if not data:
                    break
                try:
                    # Pipelining: run every complete command in this read, then
                    # flush all the replies with one drain().
                    for args in parser.feed(data):
                        reply = self.execute(client, args)
                        if reply is not NO_REPLY:
                            client.send(reply)
                        if client.closing:
                            break
                except ProtocolError as e:
                    client.send(Error(f"ERR Protocol error: {e}"))
                    client.closing = True
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self.clients.discard(client)
            for channel in list(client.subscriptions):
                self.pubsub_unsubscribe(client, channel)
            writer.close()

    # ------------------------------------------------------------------
    # Command dispatch
    # ------------------------------------------------------------------

    def execute(self, client: ClientConn, args: list[bytes], replaying: bool = False):
        name = args[0].upper()
        self.stats["total_commands_processed"] += 1

        # ---- transactions ----
        if name == b"MULTI":
            if client.in_multi:
                return Error("ERR MULTI calls can not be nested")
            client.in_multi, client.multi_queue, client.multi_error = True, [], False
            return OK
        if name == b"DISCARD":
            if not client.in_multi:
                return Error("ERR DISCARD without MULTI")
            client.in_multi, client.multi_queue = False, []
            return OK
        if name == b"EXEC":
            if not client.in_multi:
                return Error("ERR EXEC without MULTI")
            queued, failed = client.multi_queue, client.multi_error
            client.in_multi, client.multi_queue, client.multi_error = False, [], False
            if failed:
                return Error("EXECABORT Transaction discarded because of previous errors.")
            # Single-threaded: nothing else can run between these commands.
            return [self._run(client, cmd, replaying) for cmd in queued]

        cmd = COMMANDS.get(name)
        if cmd is None:
            err = Error(f"ERR unknown command '{args[0].decode(errors='replace')}', "
                        f"with args beginning with: {' '.join(repr(a.decode(errors='replace')) for a in args[1:3])}")
            client.multi_error |= client.in_multi
            return err
        if (cmd.arity > 0 and len(args) != cmd.arity) or (cmd.arity < 0 and len(args) < -cmd.arity):
            client.multi_error |= client.in_multi
            return Error(f"ERR wrong number of arguments for '{cmd.name.lower()}' command")

        if client.in_multi:
            if name in (b"SUBSCRIBE", b"UNSUBSCRIBE"):
                # These send their own out-of-band replies, which can't be
                # nested inside an EXEC reply array.
                client.multi_error = True
                return Error(f"ERR Command {cmd.name} is not allowed inside a transaction")
            client.multi_queue.append(args)
            return QUEUED

        if client.subscriptions and not cmd.pubsub_ok:
            return Error(f"ERR Can't execute '{cmd.name.lower()}': only (P|S)SUBSCRIBE / "
                         "(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")

        return self._run(client, args, replaying)

    def _run(self, client: ClientConn, args: list[bytes], replaying: bool):
        cmd = COMMANDS[args[0].upper()]
        ctx = Context(self.store, self, client)
        try:
            reply = cmd.handler(ctx, args[1:])
        except Error as e:
            return e
        if cmd.write and self.aof and not replaying:
            for logged in (ctx.propagated if ctx.propagated is not None else [args]):
                self.aof.append(logged)
        return reply

    # ------------------------------------------------------------------
    # Pub/Sub
    # ------------------------------------------------------------------

    def pubsub_subscribe(self, client: ClientConn, channel: bytes) -> None:
        self.channels.setdefault(channel, set()).add(client)
        client.subscriptions.add(channel)

    def pubsub_unsubscribe(self, client: ClientConn, channel: bytes) -> None:
        subs = self.channels.get(channel)
        if subs:
            subs.discard(client)
            if not subs:
                del self.channels[channel]
        client.subscriptions.discard(channel)

    def pubsub_publish(self, channel: bytes, message: bytes) -> int:
        subs = self.channels.get(channel, ())
        for c in subs:
            c.send([b"message", channel, message])
        return len(subs)

    # ------------------------------------------------------------------
    # Admin
    # ------------------------------------------------------------------

    def rewrite_aof(self) -> int:
        if not self.aof:
            raise Error("ERR AOF is not enabled (start with --aof FILE)")
        return self.aof.rewrite(self.store)

    def info(self) -> str:
        keys = len(self.store.keys())
        lines = [
            "# Server",
            f"mini_redis_version:{__version__}",
            f"process_id:{os.getpid()}",
            f"tcp_port:{self.port}",
            f"uptime_in_seconds:{int(time.time() - self.started_at)}",
            "",
            "# Clients",
            f"connected_clients:{len(self.clients)}",
            "",
            "# Persistence",
            f"aof_enabled:{int(self.aof is not None)}",
            f"aof_fsync:{self.aof.fsync_policy if self.aof else 'n/a'}",
            "",
            "# Stats",
            f"total_connections_received:{self.stats['total_connections_received']}",
            f"total_commands_processed:{self.stats['total_commands_processed']}",
            f"expired_keys:{self.store.expired_keys}",
            f"pubsub_channels:{len(self.channels)}",
            "",
            "# Keyspace",
        ]
        if keys:
            lines.append(f"db0:keys={keys},expires={len(self.store.expires)}")
        return "\r\n".join(lines) + "\r\n"
