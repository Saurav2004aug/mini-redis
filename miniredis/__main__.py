"""
Run:  python -m miniredis [--port 6379] [--aof appendonly.aof] [--fsync everysec]
Then use any Redis client:  redis-cli -p 6379
"""
import argparse
import asyncio
import signal

from . import __version__
from .server import MiniRedisServer


def main():
    parser = argparse.ArgumentParser(prog="miniredis", description="A Redis-compatible server built from scratch.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--aof", metavar="FILE", help="enable append-only-file persistence")
    parser.add_argument("--fsync", choices=["always", "everysec", "no"], default="everysec")
    args = parser.parse_args()

    async def run():
        server = MiniRedisServer(args.host, args.port, args.aof, args.fsync)
        port = await server.start()
        print(f"mini-redis {__version__} ready on {args.host}:{port}"
              + (f" (AOF: {args.aof}, fsync={args.fsync})" if args.aof else ""), flush=True)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)  # graceful shutdown flushes the AOF
        await stop.wait()
        print("shutting down, flushing AOF...", flush=True)
        await server.stop()

    asyncio.run(run())


if __name__ == "__main__":
    main()
