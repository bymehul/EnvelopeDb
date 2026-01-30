# EnvelopeDB Usage Guide

This guide covers everything you need to know to get **EnvelopeDB** up and running.

---

## 🛠️ Compilation

EnvelopeDB is written in Nim. For maximum performance, always compile with the `-d:release` flag and the `orc` memory manager (default in 2.0+).

```bash
# Build the standalone server
nim c -d:release src/server.nim

# Build the distributed Raft node
nim c -d:release src/raft.nim

# Build the benchmark tool
nim c -d:release src/stress_test.nim
```

---

## 🚀 Running the Database

### 1. Standalone Mode
Run a single-node server on the default port (8888):
```bash
./src/server
```

### 2. Distributed Mode (Raft Cluster)
Launch 3 nodes in separate terminal windows to form a cluster:
```bash
# Terminal 1
./src/raft 9001 --clear
# Terminal 2
./src/raft 9002 --clear
# Terminal 3
./src/raft 9003 --clear
```

---

## 📡 Interacting with EnvelopeDB

EnvelopeDB uses a simple text-based protocol (similar to Redis). You can use `netcat` (`nc`) or `telnet` to send commands.

### Using Netcat
```bash
# Connect to the server
nc localhost 8888

# Basic Commands
SET user:1 {"name":"Bob","age":30}
GET user:1
JGET user:1 name
USE bucket2
SET key1 "Hello"
GET key1
QUIT
```

---

## 💻 Programmatic Usage (Nim)

You can easily integrate EnvelopeDB into your Nim applications:

```nim
import asyncnet, asyncdispatch

proc run() {.async.} =
  let socket = newAsyncSocket()
  await socket.connect("127.0.0.1", Port(8888))
  
  # Write data
  await socket.send("SET mykey myvalue\c\l")
  echo await socket.recvLine() # Expected: +OK
  
  # Read data
  await socket.send("GET mykey\c\l")
  echo await socket.recvLine() # Expected: $7 (length)
  echo await socket.recvLine() # Expected: myvalue

waitFor run()
```

---

## 📊 Performance Testing
To verify the performance on your machine:
```bash
./src/stress_test
```
This will spawn 50 concurrent clients performing thousands of operations.
