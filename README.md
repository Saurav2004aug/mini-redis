# mini-redis — Redis-Compatible Server from Scratch

[![CI](https://github.com/Saurav2004aug/mini-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/Saurav2004aug/mini-redis/actions)

A Redis-compatible in-memory database server implemented from scratch in Python. The project focuses on **TCP networking, RESP protocol parsing, asynchronous concurrency, persistence, crash recovery, and database-style command execution**.

It works with the official `redis-cli` and supports **65+ Redis-style commands**, TTLs, transactions, pub/sub, and append-only-file (AOF) persistence.

## 🚀 Live Deployment

Mini-Redis is deployed on **Railway** as a raw TCP service.

| Component | Details |
|---|---|
| Platform | Railway |
| Public TCP endpoint | `acela.proxy.rlwy.net:56315` |
| Application port | `6379` |
| Protocol | RESP over TCP |
| Persistent storage | Railway Volume mounted at `/data` |
| AOF file | `/data/appendonly.aof` |
| AOF fsync | `everysec` |
| Runtime | Python 3.12 |
| Container | Docker |

### Live connection

Using Dockerized `redis-cli`:

```bash
docker run --rm -it redis:7-alpine redis-cli -h acela.proxy.rlwy.net -p 56315
```

Then:

```text
PING
PONG

SET name Saurav
OK

GET name
"Saurav"
```

The public endpoint above was verified with real `PING`, `SET`, and `GET` operations after deployment.

> **Note:** The public Railway TCP port (`56315`) is externally exposed by Railway and forwards traffic to the application's Redis-compatible port (`6379`).

## ✨ Highlights

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
- Compatible with official `redis-cli`
- Benchmark support with `redis-benchmark`
- 60 automated tests

## 🏗️ Architecture

```text
                   TCP / RESP
Client / redis-cli ───────────────► asyncio TCP server
                                      |
                                      v
                                RESP parser
                                      |
                                      v
                              Command dispatcher
                                /           \
                               /             \
                              v               v
                           Store             AOF
                         /   |   \             |
                        /    |    \            v
                      TTL  Data  Pub/Sub    Recovery
                                              |
                                              v
                                           Compaction
```

### Request flow

1. A client opens a TCP connection.
2. The asyncio server receives potentially fragmented TCP data.
3. The RESP parser reconstructs complete protocol frames.
4. The command dispatcher validates and executes the command.
5. Successful mutating commands are persisted according to the AOF configuration.
6. The response is encoded back into RESP and returned to the client.

The server uses a single asyncio event loop, which keeps command execution serialized and makes transaction execution deterministic without requiring command-level locks.

## 🧰 Tech Stack

- **Python 3.12**
- **asyncio**
- **TCP/IP**
- **RESP**
- In-memory data structures
- File-based AOF persistence
- `unittest`
- Docker
- Railway

## ▶️ Run Locally

### Option 1 — Python

```bash
python -m miniredis
```

The server listens on the configured Redis-compatible port.

Connect with the official CLI:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

### Option 2 — Docker

Build:

```bash
docker build -t mini-redis .
```

Run:

```bash
docker run --rm --name mini-redis-test -p 6379:6379 mini-redis
```

Connect from another terminal:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

If `redis-cli` is not installed locally, use Docker:

```bash
docker run --rm -it --network host redis:7-alpine redis-cli -p 6379
```

## 💡 Example

Basic key/value operations:

```text
SET name Saurav
GET name
DEL name
```

TTL:

```text
SET session abc EX 60
TTL session
```

Transactions:

```text
MULTI
SET counter 10
INCR counter
EXEC
```

The project also supports Redis-style lists, hashes, sets, pub/sub, and additional commands.

## 💾 Persistence & Crash Recovery

Mini-Redis uses an **Append-Only File (AOF)** for persistence.

The deployed service stores the AOF at:

```text
/data/appendonly.aof
```

The `/data` directory is backed by a Railway persistent volume, so the AOF survives container restarts and redeployments.

### Durability behavior

- Successful mutating commands can be appended to the AOF.
- The default deployed fsync policy is `everysec`.
- Relative TTLs can be persisted as absolute expiration information so recovery preserves the intended expiration time.
- Startup recovery replays valid AOF records.
- An incomplete final write caused by a crash can be detected and safely discarded/truncated.
- AOF compaction rewrites the current state into a smaller recoverable log.

This makes the project useful for demonstrating the trade-off between **durability, recovery, and write throughput**.

## 🧪 Testing

Run the automated test suite:

```bash
python -m unittest discover -s tests -v
```

The suite covers:

- RESP parsing
- command execution
- storage
- TTL behavior
- transactions
- AOF persistence
- AOF recovery

Current project test suite: **60 automated tests**.

## 📊 Performance Benchmark

Mini-Redis includes support for benchmarking with the standard Redis benchmark client:

```bash
redis-benchmark -h 127.0.0.1 -p 6379
```

For a controlled benchmark, keep the same:

- machine / CPU
- Docker configuration
- concurrency
- request payload
- command mix
- persistence configuration

when comparing runs.

> Benchmark throughput is environment-dependent. Treat benchmark numbers as measurements of the specific test environment rather than as a universal server limit.

## 🔐 Deployment Notes

### Railway

The production deployment uses:

```text
Docker image
    ↓
Railway service
    ↓
TCP Proxy
    ↓
public-host:public-port
    ↓
Mini-Redis :6379
    ↓
Railway Volume /data
    ↓
appendonly.aof
```

Important deployment details:

- The application listens on `0.0.0.0:6379`.
- Railway TCP Proxy exposes the service externally.
- Railway assigns the public TCP port; it does **not** need to match `6379`.
- Persistent storage is mounted at `/data`.
- `RAILWAY_RUN_UID=0` is configured so the container can initialize permissions on the mounted volume before starting the application user.
- A Docker `VOLUME` instruction is intentionally not used because Railway manages persistent volumes separately.

## 🧠 Design Decisions

### Async TCP server

Python `asyncio` allows many client connections to be handled without creating one OS thread per connection.

### Single event loop

Command execution is serialized through the event loop. This simplifies atomic operations and transaction semantics without requiring locks around the shared in-memory store.

### RESP parser

TCP is a byte stream: one `read()` is not guaranteed to contain exactly one request. The parser therefore handles:

- fragmented TCP reads
- multiple commands in one read
- RESP framing
- binary-safe payloads

### TTL expiration

Expiration combines command-time checks with background/active cleanup so expired keys do not remain indefinitely.

### AOF persistence

AOF provides a simple durability model while making the trade-off between fsync frequency and throughput explicit.

## 🎯 Why This Project Matters

This project demonstrates lower-level engineering rather than CRUD application development:

- TCP networking
- protocol framing
- asynchronous I/O
- state management
- concurrency
- persistence
- crash recovery
- durability/performance trade-offs
- containerization
- production deployment
- debugging a real volume-permission issue

## 🎤 Interview Topics

Be prepared to explain:

- Why TCP reads cannot be assumed to return a complete request
- How RESP framing works
- How fragmented/pipelined requests are handled
- Why `asyncio` is suitable for many concurrent connections
- How command atomicity is maintained
- How `MULTI` / `EXEC` works
- TTL implementation strategies
- AOF durability vs throughput
- `fsync=everysec` trade-offs
- Crash recovery and torn writes
- AOF compaction
- Why relative expiration needs careful persistence
- How Docker and Railway volumes interact
- How the production volume-permission issue was diagnosed and fixed

## 📁 Project Structure

```text
mini-redis/
├── miniredis/
│   ├── __main__.py
│   ├── server.py
│   ├── resp.py
│   ├── commands.py
│   ├── store.py
│   └── aof.py
├── tests/
├── docs/
├── Dockerfile
├── requirements.txt
└── README.md
```

## 📌 Project Summary

**Mini-Redis** is a Redis-compatible server built from scratch in Python, covering TCP networking, RESP parsing, asynchronous concurrency, in-memory storage, TTLs, transactions, pub/sub, AOF persistence, crash recovery, Dockerization, and production TCP deployment on Railway.
