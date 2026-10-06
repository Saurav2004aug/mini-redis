# mini-redis — Redis-Compatible Server from Scratch

[![CI](https://github.com/YOUR_GITHUB_USERNAME/mini-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GITHUB_USERNAME/mini-redis/actions)

A Redis-compatible in-memory database server implemented from scratch in Python. The project focuses on **TCP networking, protocol parsing, concurrency, persistence and database-style command execution**.

It works with the official `redis-cli` and supports a substantial Redis-style command set, TTLs, transactions, pub/sub and append-only-file persistence.

## Highlights

- TCP server implemented with Python `asyncio`
- RESP protocol parser
- 65+ Redis-style commands
- Binary-safe values
- TTL and key expiration
- Transactions with `MULTI` / `EXEC`
- Pub/sub
- Append-only-file persistence
- Configurable fsync policy
- Recovery from partial/torn AOF writes
- AOF compaction
- Compatible with `redis-cli`
- Benchmark support with `redis-benchmark`
- 60 automated tests

## Architecture

```text
redis-cli / client
       |
       | TCP
       v
  RESP parser
       |
       v
Command dispatcher
       |
  +----+----+
  |         |
Store      AOF
  |         |
 TTL       Recovery
  |
Pub/Sub
```

## Tech Stack

- Python
- asyncio
- TCP/IP
- RESP
- In-memory data structures
- File-based persistence
- unittest
- Docker

## Run locally

```bash
python -m miniredis
```

The server listens on the configured Redis-compatible port.

Using the official CLI:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

Docker:

```bash
docker build -t mini-redis .
docker run --rm -p 6379:6379 mini-redis
```

## Example

```text
SET name Saurav
GET name
SET session abc EX 60
TTL session
MULTI
SET counter 10
INCR counter
EXEC
```

## Persistence

The server uses an append-only log for durability. Startup recovery replays valid records and tolerates an incomplete final write, which can occur if a process crashes while appending.

Compaction rewrites the current state into a smaller AOF while preserving recoverability.

## Testing

```bash
python -m unittest discover -s tests -v
```

The suite covers RESP parsing, commands, storage, TTL behavior, transactions and AOF persistence/recovery.

## Benchmarking

```bash
redis-benchmark -h 127.0.0.1 -p 6379
```

See the benchmark/demo assets under `docs/`.

## Why this project matters

This project demonstrates lower-level engineering rather than CRUD application development:

- partial TCP reads
- protocol framing
- asynchronous I/O
- state management
- persistence and crash recovery
- durability/performance trade-offs
- concurrency

## Interview topics

- Why TCP reads cannot be assumed to return a complete request
- RESP framing and binary-safe parsing
- TTL implementation strategies
- transaction semantics
- AOF durability vs throughput
- crash recovery and torn writes
- why async I/O is useful for many concurrent connections
