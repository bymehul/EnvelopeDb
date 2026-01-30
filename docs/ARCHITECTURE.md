# EnvelopeDB Architecture

This document provides a deep dive into the technical design of **EnvelopeDB**.

---

## 💾 Storage Engine: Hybrid Bitcask/LSM-Tree

EnvelopeDB uses a specialized hybrid architecture to maximize both write throughput and disk scalability.

### 1. The Write Path (LSM-Tree)
- **MemTable**: All incoming writes are initially stored in a **SkipList** MemTable. This provides $O(\log N)$ sorted insertion with no locking overhead.
- **SSTables**: Once the MemTable reaches a size limit, it is flushed to a **Sorted String Table (SSTable)** on disk. These files use the **`.sst`** extension.
- **Bloom Filters**: Each SSTable contains a Bloom Filter in its header. This allows us to definitively say "this key is NOT in this file" without touching the disk, preventing unnecessary read IOPS.

### 3. File Formats & Extensions
EnvelopeDB uses specific extensions to manage its data layout:
- **`.evp`**: Persistent **Write-Ahead Log (WAL)** files containing the raw append-only data.
- **`.hint`**: **Index files** that map keys to their specific offsets in `.evp` files for $O(1)$ startup.
- **`.sst`**: **LSM-Tree segments** containing sorted key-value pairs for high-capacity storage.
- **`.meta`**: Internal **Raft state** (Current Term, Vote) to ensure cluster safety across restarts.

### 2. The Read Path (Zero-Copy mmap)
- **Memory Mapping**: We use `mmap` for reading immutable data files. This allows the OS kernel to manage data caching directly, providing **hardware-level zero-copy speed**.
- **Hint Files**: On startup, EnvelopeDB reads `.hint` files to rebuild the in-memory index in seconds, even for millions of keys.

---

## 🌐 Distributed Consensus: Raft

EnvelopeDB achieves High Availability (HA) through a from-scratch implementation of the Raft protocol.

### 1. Leader Election
Nodes heartbeat each other to maintain a stable leader. If the leader fails, a randomized election timer triggers a new vote.

### 2. Quorum-Based Replication
- A command is only considered "committed" when a **majority (Quorum)** of nodes have safely written it to their persistent logs.
- This prevents "split-brain" scenarios and ensures data consistency even after a node crash.

### 3. State Machine Persistence
The Raft state (Current Term, VotedFor, Log) is persisted to `raft.meta`. This allows a node to reboot and immediately re-join the cluster with its full history intact.

---

## ⚡ Performance Optimizations
- **Sequential Writes**: By only appending data, we avoid the mechanical seek-time bottleneck of HDDs. 
- **CRC32 Validation**: Every record has a checksum to guarantee that silent disk corruption is detected immediately. 
- **Namespace Buckets**: Logical buckets allow O(1) isolation between different data tenants.
