# Write-Ahead Log (WAL) with io_uring

A production-grade write-ahead log implementation in Zig, designed to survive real hardware failures.

## Overview

This is a dual write-ahead log system with:

- **CRC32C checksums** on every record for corruption detection
- **Dual WAL files** (primary and secondary) for latent sector error (LSE) protection
- **O_DIRECT + O_DSYNC** for guaranteed persistent storage
- **Linked io_uring operations** (write→fsync chains) for proper I/O ordering
- **Post-fsync verification reads** to detect silent failures before they matter
- **Log recovery and replay** to reconstruct state after restart

**For the complete design rationale and why each layer matters, read:** [Disks Lie: Building a Write-Ahead Log That Actually Survives](https://blog.canoozie.net/disks-lie-building-a-wal-that-actually-survives/)

## Building

```bash
zig build
```

## Running Tests

```bash
zig test src/record.zig
zig test src/io_manager.zig
zig test src/verification.zig
zig test src/recovery.zig
zig test src/wal.zig
```

All tests pass (56 tests total across all modules).

## Architecture

- **record.zig** - Record serialization with CRC32C checksums
- **wal.zig** - Core WAL orchestration and pending operation tracking
- **io_manager.zig** - io_uring submission and completion handling with linked operations
- **verification.zig** - Post-fsync verification and LSE detection
- **recovery.zig** - Log recovery and dual-file reconciliation
- **root.zig** - Public API exports
- **main.zig** - Demo CLI

## Key Design Decisions

1. **Dual redundancy isn't optional** - LSEs happen in production. One copy is negligence.
2. **Checksums on every record** - Silent corruption is real. Verify everything.
3. **O_DIRECT by default** - The page cache is not your friend for durability.
4. **io_uring linked operations** - Maintain ordering without losing concurrency.
5. **Verification reads** - Catch LSE failures while recovery is still possible.

## Use Cases

- Building durable key-value stores
- Distributed system replication logs
- Transaction logs for ACID compliance
- Any system where data loss is unacceptable

## License

MIT
