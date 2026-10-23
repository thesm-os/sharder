---
rfc: 0014
title: A filesystem store for the scopes of one process
author: Roy Klopper
status: Draft
created: 2026-10-13
updated: 2026-10-14
discussion: none
supersedes: none
superseded-by: none
produces-adr: tbd
research: none
related: RFC-0007, RFC-0009, RFC-0015
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# RFC-0014: A filesystem store for the scopes of one process

## Summary

`store/memory` keeps every scope in the memory of its process, so a process that restarts loses its namespaces with their epochs and watermarks. We propose `store/file`, which keeps the scopes of one OS process in a local directory. It implements `store.Store` and `membership.Membership`, and provides the capabilities `durable` and `leases`. Each call that changes a scope appends one record to a write-ahead log and syncs it before the call returns. A checkpoint bounds the log.

The store keeps every call that returned through a crash of the process or of the machine, if the disk returns the bytes of each completed sync unchanged. It refuses to open a directory when a later record proves that synced bytes are damaged. It cannot tell damage to the last record of the log from an append that a crash interrupted, so it truncates that record.

`store/file` and `store/memory` share one state machine of scopes, sessions and elections. The store is not replicated. A namespace whose store loses its disk recovers with a new namespace incarnation.

## Motivation

### A restart over store/memory

`controller.New` refuses a store without the capability `durable` unless the process sets `Volatile`, and `store/memory` provides only `leases`. A process over `store/memory` that restarts finds every scope empty. Its root creates the namespace again with a new incarnation.

RFC-0007 states what a lost store costs: "A namespace recovers from the loss of its store with a new namespace incarnation, over a fresh application-data namespace or behind an administrative barrier that stops every writer of the old incarnation." Over `store/memory`, every restart of a process is such a loss. A durable store removes the loss for a process that restarts on the same disk.

### The laws of a store

RFC-0007 lists the laws that every store passes in `store/storetest`. A single process on one local disk meets the laws that concern safety:

- Law 10: a commit that returned a generation is readable, with the same generation and receipt, after the store is opened again.
- Law 11: `Commit` checks the incarnation, the fence, the receipt and the generation in that order, in one atomic step.
- Law 8: `Campaign` grants each term of an election to one candidate.

Replication does not add to these laws. It keeps the control plane available while one machine fails, and it keeps the scopes when one disk is lost. While the store is unavailable, writers under `continue-fenced` serve, routers route on their last routes, and no step that needs a commit advances.

### Crash consistency

A store that is durable on a process restart can still lose acknowledged state on a power loss:

- A sync of a file does not make the entry of the file in its directory durable. The Linux manual of `fsync` requires a sync of the directory as well. Pillai et al. found that 7 of the 11 applications that they studied did not meet the durability that users likely expect, often because the applications did not flush a directory operation.
- A crash can persist a part of an append that spans more than one block, on every filesystem that Pillai et al. tested. On ext4 with `data=writeback`, the extended part of a file can contain stale data after a crash.
- A failed sync cannot be retried. PostgreSQL found that a later sync of the same file can report success for data that the kernel discarded after the failure.
- The decoder of an older release skips the fields that it does not know, because kanon's wire format requires a decoder to skip an unknown field number. A release that replays the records of a newer release rebuilds another state without an error.

### Prior art

- PostgreSQL protects each WAL record with a CRC-32C, and syncs every file of the data directory before crash recovery begins. Crash recovery ends at the first invalid record and writes new WAL from there, whether a crash or damage in the middle of the WAL made the record invalid.
- etcd treats a record of its last segment that fails its checksum as a torn write when one 512-byte sector of the record contains zeros alone. It truncates the segment before such a record, and refuses to start on any other checksum failure. It creates a new log in a temporary directory, renames the directory into place and syncs its parent.
- Pebble tests its crash recovery against an in-memory filesystem. A crash clone of it keeps the bytes and the directory entries that a sync covered, and a random part of the rest. Pebble's `errorfs` fails one chosen call to the filesystem at a time.
- RocksDB offers four recovery modes. Its default stops the replay before the first damaged record. The mode that tolerates a damaged tail accepts an incomplete record or zeros at the tail, and refuses a checksum failure there, because it cannot tell that failure from damage.
- SQLite assumes that the data that it reads is the data that it wrote. It leaves the detection of bit errors to the hardware and the operating system.
- `store/memory` keeps the term of each election and the incarnation of each node in counters (`election.last` and `members.last`). A store that keeps its scopes across a restart must also keep both counters, or it grants a term or an incarnation twice.

## Detailed design

### Components

| Component | Package | Responsibility |
|---|---|---|
| State machine | `store/internal/state`, new | The scopes with their generations, receipts, reads and watches, the sessions and views of the membership, and the elections. It calls a journal before each change takes effect |
| Memory store | `store/memory` | The state machine with a journal that records nothing, and the fault points of the tests |
| File store | `store/file`, new | The state machine with a journal over a directory: the lock, the log, the checkpoints and the recovery |
| Filesystem | `store/file/internal/fsys`, new | The calls of the file store to the operating system. One implementation calls the operating system, and one models a crash for the tests |

`store/memory` keeps its exported API. Its unexported scopes, sessions and elections move into `store/internal/state`, which both stores import. No package of core provides a write-ahead log or a filesystem with syncs: `blob` and `cas` are object stores without appends or syncs.

### The API

```go
// Package file is a store.Store and a membership.Membership that keep the scopes of
// one OS process in a local directory.
package file

// MaxRecord is the most bytes of one record of the log, its frame included: 64 MiB.
const MaxRecord = 67108864

// Open opens the store in dir. It creates dir and each missing parent, and syncs the
// parent of each directory that it creates. It locks dir for the process, checks the
// format and the schema of every file, recovers the state and starts a new segment of
// the log. The leases, the timers and the progress of the watches run on clk.
//
// Error modes:
//   - an error that wraps ErrLocked when another process has locked dir.
//   - an error that wraps ErrUnsupported for a file of a newer format, a record of an
//     unknown kind, or a schema above store.Schema. Open has not changed any file.
//   - an error that wraps ErrCorrupt for damage that the format or a later record
//     proves. Open has not changed any file.
//   - an error that wraps the error of ctx, or of a call to the operating system.
func Open(ctx context.Context, dir string, clk clock.Clock, opts ...Option) (*Store, error)

// WithLimits sets the bounds of every commit, which Limits returns and Fits applies.
// Open refuses a MaxBytes that a record of MaxRecord bytes cannot contain.
func WithLimits(l store.Limits) Option

// WithCheckpointAt sets the length of the log since the published snapshot at which
// the store starts a checkpoint.
func WithCheckpointAt(bytes int64) Option

// WithLogLimit sets the length of the log since the published snapshot at which a
// change waits for the running checkpoint. Open refuses a limit at or below the length
// of WithCheckpointAt.
func WithLogLimit(bytes int64) Option

// WithHistory sets the number of generations of each scope that the store keeps for
// its watchers.
func WithHistory(generations int) Option

// Provides returns the capabilities durable and leases.
func (*Store) Provides() []config.Capability

// Membership returns the membership of the store.
func (*Store) Membership() *Membership

// Delete removes scope s, as an operator does before a namespace is created again
// under the same name. It returns once the removal is durable.
func (*Store) Delete(ctx context.Context, s shard.Scope) error

// Close ends every session, term and watch, waits for the running checkpoint, and
// releases the lock.
func (*Store) Close() error
```

The defaults are a `MaxBytes` of 16 MiB and no bound on `MaxPlacements`, a checkpoint at 64 MiB of log, a log limit of 256 MiB, and 1,024 generations of history per scope.

### The fault model

The store keeps its guarantees under two failures:

- The process ends at any instruction.
- The machine loses power, or its operating system crashes.

After either failure, a file contains each byte that a completed sync of the file covered, unchanged. A byte that the store wrote after the last completed sync of its file can be absent, written, zero or stale. A directory contains each entry that a completed sync of the directory covered, and a rename leaves either the old entry or the new one. The fault harness of the tests models exactly these outcomes.

The store detects these failures and refuses to open:

- damage to the snapshot or to a sealed segment
- damage to a record of the last segment when a valid later record follows it
- a missing segment, or a gap in the sequence numbers of segments or records

The store does not cover these failures:

- Damage to the last record of the log. `Open` cannot tell it from an append that a crash interrupted, so it truncates the record. The call of that record is lost if it had returned.
- A disk that reports a sync as complete before its data is durable.
- A power loss on Windows. Microsoft does not document that a sync of a directory, or a rename with `MOVEFILE_WRITE_THROUGH` within one volume, makes a directory entry durable.
- The loss of the disk, a restore of the directory from a backup, and a directory on a network filesystem.

### The directory

| File | Contains |
|---|---|
| `LOCK` | Nothing. The process that opened the store has an exclusive lock on it |
| `log-<n>` | Segment `n` of the log, with `n` in 16 hexadecimal digits |
| `snapshot` | The published checkpoint: the state of every scope and the counters after a named record |
| `snapshot.tmp` | A checkpoint that the store has not published. `Open` removes it |

`Open` ignores a file whose name the store does not use.

### Formats

Every integer is little-endian, and every checksum is a CRC-32C. A segment starts with a header of 48 bytes:

| Field | Bytes | Contains |
|---|---|---|
| Magic | 8 | The ASCII bytes `SHARDLOG` |
| Format | 4 | The version of the format, 1 |
| Store | 16 | The identity of the store: 16 random bytes that the bootstrap generates |
| Segment | 8 | The sequence number of the segment |
| First | 8 | The sequence number of the first record of the segment |
| Checksum | 4 | The checksum of the 44 bytes before it |

Records follow the header. Each record has a frame of 21 bytes around its payload:

| Field | Bytes | Contains |
|---|---|---|
| Sequence | 8 | The sequence number of the record, one above the record before it, across segments |
| Kind | 1 | The kind of the record |
| Length | 4 | The length of the payload, at most `MaxRecord` minus 21 |
| Header checksum | 4 | The checksum of the identity of the store and of the 13 bytes before it |
| Payload | Length | The kanon encoding of the record |
| Payload checksum | 4 | The checksum of the payload |

The header checksum makes the length trustworthy before `Open` reads the payload. The bound on the length also bounds the memory of a read. Stale bytes on the disk come from a deleted segment of the same store, whose sequence numbers are below the next one, or from another store, whose identity differs. Neither forms a valid record at the position of the next record. RocksDB adds the number of a log to the checksum of each record of a recycled log for the same reason.

| Kind | Contains | Written by |
|---|---|---|
| Commit | The scope, the namespace incarnation and the term of the writer, the schema of the release that wrote the record, the generation that the commit created, the receipt and the mutation | `Commit`, after every check passed |
| Delete | The scope | `Delete` |
| Term | The scope and the term | An election, before it grants the term |
| Incarnation | The scope, the node and the incarnation | `Join`, before it starts the session |

A snapshot starts with a header of 48 bytes in the layout of a segment header. Its magic is `SNAPSHOT`. In place of the segment and the first record, the header contains the first segment that the snapshot does not cover and the sequence number of the last record that it covers. One frame per scope follows, in the layout of a record. Each of these frames contains the head, the state and the placements of the current generation, the receipts, the last term of the election and the last incarnation of each node. A final frame contains the number of scopes, and a snapshot without it is damaged.

The format version is 1. A release that changes a frame, a header or a kind raises it.

### Bootstrap

`Open` makes each step durable before a later step depends on it:

1. It creates `dir` and each missing parent, from the outermost, and syncs the parent of each directory that it creates.
2. It creates `LOCK` and locks it.
3. In a directory without a segment and without a snapshot, it generates the identity of the store with `crypto/rand`, writes the header of segment 1, syncs the segment and syncs the directory.

The store appends a record to a segment only after it has synced the header of the segment and the directory. A call can return only after `Open` has returned, so every record of a call is in a segment whose entry in the directory is durable. The store syncs the directory after it creates each later segment too.

### Changes and their records

A change is a call that writes a record: `Commit`, `Delete`, `Join`, and the grant of a term. The store has a journal lock and a state lock. The journal lock makes the changes run one at a time. The state lock protects the state machine, as the lock of `store/memory` does. A change takes the journal lock before the state lock.

A change runs in four steps:

1. Under both locks, the state machine runs the checks of the call and builds its record.
2. With the state lock released, the store appends the record to the current segment and syncs the segment.
3. Under the state lock, the state machine applies the change.
4. The call releases both locks and returns.

Reads, watches, renewals and the expiry of leases take the state lock alone, so they never wait for I/O. The checks of a change read the incarnation, the watermark, the receipts and the generation of a scope. Reads, renewals and expiries do not alter these values. The journal lock keeps every other change out between step 1 and step 3, so the checks of step 1 remain true at step 3.

| Change | Record | When its write or sync fails |
|---|---|---|
| `Commit` | Commit | It returns an error that wraps `version.ErrOutcomeUnknown` and `ErrFailed` |
| `Delete` | Delete | It returns an error that wraps `version.ErrOutcomeUnknown` and `ErrFailed` |
| `Join` | Incarnation | It returns an error that wraps `ErrFailed`. The incarnation may be on the disk, and a later `Join` gets a higher one |
| The grant of a term | Term | Each waiting `Campaign` returns an error that wraps `ErrFailed`. The term may be on the disk, and a later grant is higher |

A failed write, sync, truncation, rename or removal fails the store closed. Every later call returns an error that wraps `ErrFailed`. The store ends every session and every term. It ends every watch and every waiting `Campaign` with an error that wraps `ErrFailed`. The process then opens the store again. The store never retries a failed sync. A later sync of the same file can report success for data that the kernel discarded.

A change waits for the journal lock under its context. When the context ends before step 1, the change returns the error of the context. For `Commit` that error also wraps `version.ErrOutcomeUnknown`, as in `store/memory`. Once step 2 has started, the change completes steps 2 and 3 and returns their result. Its context no longer applies.

### Sessions and elections

Sessions, leases, views and terms in progress remain in memory on the clock of the store, as in `store/memory`. A restart of the process ends every session and every term. Every caller of the store restarts with the process.

`Join` writes the incarnation of the node before it starts the session. An election writes the term before it grants it. It grants a term when a term ends or when a candidate arrives at a scope without a leader. A term can end on the goroutine of the timers, so the store grants each term on a goroutine of its own, as a change. The incarnations of a node and the terms of an election then rise across restarts.

### Checkpoints

A checkpoint writes the state into a new snapshot and removes the segments that the snapshot covers. The store starts one when the log since the published snapshot grows past the length of `WithCheckpointAt`, and runs one checkpoint at a time:

1. Under the journal lock, the store seals segment `n`, writes the header of segment `n+1`, syncs it and syncs the directory. It then captures each scope under the state lock: the head, the current generation, a copy of the receipts, the last term of the election and a copy of the last incarnation of each node. Only a change alters these values, so the capture is the state after the last record of segment `n`. A generation never changes after its commit, so the capture refers to it. The receipts and the counters change in place, so the capture copies them.
2. Without a lock, a goroutine of the store encodes the capture into `snapshot.tmp`, syncs it, renames it to `snapshot` and syncs the directory.
3. The store removes the segments up to `n` and syncs the directory.

A crash before the sync of the directory in step 2 leaves the old snapshot or the new one, with every segment that each of them needs. `Open` removes a segment that the snapshot covers. The store removes a segment only after the rename of a snapshot that covers it is durable in the directory.

Changes continue in segment `n+1` while a checkpoint runs. A change waits when the log since the published snapshot has grown past the length of `WithLogLimit`, until the checkpoint publishes its snapshot. The context of the change bounds the wait. The limit bounds the log that `Open` replays.

### Recovery

`Open` recovers in three passes, and does not change a file before the third:

1. It locks the directory, lists the files, and syncs every segment, the snapshot and the directory. A process that crashed can leave bytes that the kernel has not written yet. The syncs make them durable, so `Open` replays only durable bytes. A failed sync fails `Open`.
2. It reads the snapshot and the segments after it in sequence order. It checks every header, every frame, every kind and every schema, and replays each record through the state machine without the journal and without `Fits`. A Commit record applies at the generation that it records, and raises the schema of its scope to the schema that it records. A record whose generation does not follow the generation of its scope returns `ErrCorrupt`.
3. It truncates or removes the last segment by the tail rule, removes `snapshot.tmp` and the segments that the snapshot covers, and syncs each segment that it truncated and the directory. It then writes the header of a new segment, syncs it, syncs the directory and returns.

A record is valid when its header checksum, its sequence number and its payload checksum are correct, and its frame ends inside the file. The tail rule classifies an invalid record and an invalid segment header:

- In the snapshot or in a sealed segment, an invalid record is damage, and `Open` returns `ErrCorrupt`. A sealed segment contains only records that the store synced before it created the next segment.
- In the last segment, an invalid record is damage when a valid record with a higher sequence number follows it, and `Open` returns `ErrCorrupt`. `Open` searches the rest of the segment for such a record.
- Any other invalid record of the last segment is an append that a crash interrupted. Its call did not return, because the store syncs each record before its call returns and writes one record at a time. `Open` truncates the segment before the record.
- A last segment of at most 48 bytes with an invalid header is a creation that a crash interrupted, and `Open` removes it. Every other invalid segment header is damage.

`Open` skips one value of each counter. The next term of an election is two above the last term that the files record, and the next incarnation of a node is two above its last recorded incarnation. A term or an incarnation of a truncated record is then never granted again, also when media damage truncated a record whose call had returned.

After `Open`, the history of each scope starts at its current generation. A watch from an older generation receives `store.ErrCompacted` and reads the scope again.

### Schemas and downgrades

A Commit record contains the schema of the release that wrote it, and a snapshot contains the schema of each scope in its head. `Open` raises the schema of a scope to the recorded schema, and never to `store.Schema` of the release that opens the directory. A controller then refuses to acquire a scope that a newer release wrote.

The decoder of an older release skips the fields that it does not know. It cannot replay the records of a newer release, and its checkpoint would rewrite the state without those fields. `Open` refuses the whole directory with `ErrUnsupported` when it finds a file of a newer format, a record of an unknown kind, or a schema above `store.Schema`. It finds them in pass 2, before it changes any file.

A newer release opens the directory of an older one, and writes its own records and snapshots in its own format. To roll back a release that wrote a newer schema, an operator starts the newer release again, or recovers the namespace with a new incarnation.

### The lock

`Open` takes an exclusive lock on `LOCK` that does not wait:

- On Linux and macOS, `unix.Flock` with `LOCK_EX|LOCK_NB`, from `golang.org/x/sys/unix`.
- On Windows, `windows.LockFileEx` with `LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY`, from `golang.org/x/sys/windows`.

The operating system releases the lock when the process ends, so a crash does not leave a stale lock. A second process that opens the directory gets `ErrLocked`. The standard library of Go 1.27.2 has no file lock. `golang.org/x/sys` is in the module graph of sharder through `go.thesmos.sh/testkit`, and `store/file` makes it a direct requirement.

### Operating systems and filesystems

| System | Sync of a file | Sync of a directory | Rename |
|---|---|---|---|
| Linux, with ext4, XFS or btrfs | `fsync` | `fsync` of the directory | `rename` |
| macOS, with APFS | `fcntl` with `F_FULLFSYNC`, which `os.File.Sync` uses | The same call on the directory | `rename` |
| Windows, with NTFS | `FlushFileBuffers` | `FlushFileBuffers` on a handle of the directory with write access, which `os.OpenFile` opens with `FILE_FLAG_BACKUP_SEMANTICS`, as etcd does | `MoveFileEx` with `MOVEFILE_WRITE_THROUGH` |

The tests kill a child process during writes on all three systems. The fault harness models a power loss on every system, and no test cuts the power of a real disk. On Windows the guarantee covers the end of a process. `os.Rename` does not pass `MOVEFILE_WRITE_THROUGH`, so the store calls `windows.MoveFileEx` from `golang.org/x/sys/windows`. Pebble and RocksDB skip the sync of a directory on Windows, and etcd performs it. `Open` does not identify the filesystem. A network filesystem, and every filesystem outside the table, is outside the guarantee.

### Errors

| Error | Class | Returned by |
|---|---|---|
| `ErrLocked` | `errs.Conflict` | `Open`, when another process has locked the directory |
| `ErrCorrupt` | `errs.Integrity` | `Open`, for damage that the format or a later record proves |
| `ErrUnsupported` | `errs.Unsupported` | `Open`, for a file of a newer format, a record of an unknown kind, or a schema above `store.Schema` |
| `ErrFailed` | `errs.Invalid` | The call whose write, sync, truncation, rename or removal failed, and every call after it |
| `ErrClosed` | `errs.Invalid` | Every call after `Close` |

### Invariants

1. A change returns only after its record, and the entry of its segment in the directory, are durable.
2. Under the fault model, `Open` keeps every record whose call returned.
3. `Open` replays the records in sequence order through the state machine that applied them first, without the journal and without the bounds of the current options.
4. `Open` does not change a file before it has checked the format, the kinds, the checksums and the schemas of every file.
5. The terms of an election and the incarnations of a node rise across restarts, and a value of a truncated record is never granted again.
6. The log since the published snapshot never exceeds the log limit by more than one record.
7. One process at a time opens a directory.
8. `store/memory` and `store/file` run the same checks of a change.
9. A call never waits for I/O while it has the state lock.

### Bounds

| Quantity | Bound |
|---|---|
| I/O of `Commit`, `Delete`, `Join` and the grant of a term | One write and one sync of the segment |
| I/O of a new segment | One write, one sync of the segment and one sync of the directory |
| I/O of a read, a watch and a renewal | None |
| One record | `MaxRecord`, 64 MiB |
| Log since the published snapshot | The log limit plus one record |
| Disk | The snapshot, a `snapshot.tmp` of about the same size, and the log limit plus one record |
| Recovery | A sync of each file, a read of the snapshot, and a replay of at most the log limit plus one record |
| Memory | The current generation of each scope, and up to the history bound of earlier generations |

We have not measured the latency of a sync, the time of a recovery, or the memory of a large scope. The benchmarks of the verification measure them.

### Failure handling

| Failure | Detected by | Action |
|---|---|---|
| The process ends, or the machine loses power | Nothing in the store | `Open` replays the log. Every call that returned is kept |
| The last record of the last segment is invalid, and no valid record follows it | `Open` | `Open` truncates the segment before the record |
| A record of the last segment is invalid, and a valid record follows it | `Open` | `Open` returns `ErrCorrupt` |
| A record of a sealed segment, a segment header or the snapshot is invalid | `Open` | `Open` returns `ErrCorrupt` |
| A segment or a record is missing from the sequence | `Open` | `Open` returns `ErrCorrupt` |
| A file of a newer format, a record of an unknown kind, or a newer schema | `Open` | `Open` returns `ErrUnsupported`, and every file remains unchanged |
| A write, sync, truncation, rename or removal fails, also on a full disk | The call | The store fails closed, and the process opens it again |
| A checkpoint fails | The checkpoint | The store fails closed. The published snapshot and every segment after it remain |
| A second process opens the directory | The lock | `Open` returns `ErrLocked` |
| The disk is lost, or the directory is restored from a backup | Not detected | The namespace recovers with a new incarnation |

After `ErrCorrupt`, the namespace recovers with a new incarnation, over a fresh application-data namespace or behind an administrative barrier.

### Verification

The tests of the store cover the conformance suites, the fault model and the bounds:

- `store/storetest` with `WithReopen`, `WithDelete` and `WithCompact`, and `membership/membershiptest`. A wrapper in the tests loses the request or the reply of a commit for `WithLostCommits`, so the API of the store has no fault points.
- The tests of `store/memory` and its conformance runs, unchanged, after its state machine moves into `store/internal/state`.
- A fault harness over the crash model of `fsys`, after Pebble's crash clone and `errorfs`. The model keeps the synced bytes and entries of each file and directory. At a crash it applies one outcome to everything else: absent, written, cut short, zero or stale. The harness also fails each call to `fsys`, one call per run.
- A property crashes generated sequences of calls at every call to `fsys`. After `Open`, the state contains every call that returned, and no record after a truncated tail.
- A second property takes a checkpoint at every point of generated sequences of calls. The state after `Open` equals the state after a replay of the whole log, in the heads, the receipts, the index of placements by shard and the counters.
- Crashes at each step of the bootstrap: after the creation of the directory and of the segment, after the write, after the sync of the file, after the sync of the directory, and after `Open` returned.
- Crashes, failed writes and failed syncs in `Commit`, `Delete`, `Join` and a grant, with every counter checked after `Open`.
- Crashes at each step of a checkpoint, changes that wait at the log limit, and a full disk while the store writes `snapshot.tmp`.
- Cases of damage, apart from the crash cases: the last Commit, Term and Incarnation record, an earlier record, the length of a frame, a segment header and the snapshot. Each case asserts the outcome of the tail rule.
- Cases of versions: a record of schema 2 after a snapshot of schema 1, opened by a release of schema 1. A record of an unknown kind, and a snapshot of a newer format. Each refusal leaves every file unchanged.
- A second process gets `ErrLocked`, and the lock is free after a kill of the first one. The second process is a child process of the test binary, on Linux, macOS and Windows.
- Benchmarks of `Commit`, of a checkpoint under changes, and of `Open` over a log at its limit. They report the latency of a sync, the wait for the journal lock, the memory and the disk.

### Verdicts

- **In:** `store/file` in the root module, with a direct requirement on `golang.org/x/sys` for the lock and the rename on Windows.
- **In:** one process per directory, under an exclusive lock.
- **In:** the fault model of a crash that leaves the synced bytes intact, and the tail rule.
- **In:** a write-ahead log with one sync per change, a sequence number in every frame, and a header checksum beside the payload checksum.
- **In:** a sync of every file before recovery, and a new segment at every `Open`.
- **In:** checkpoints from immutable generations, one at a time, with a log limit at which changes wait.
- **In:** I/O outside the state lock, with a journal lock that orders the changes.
- **In:** durable counters of terms and incarnations, with one value skipped at every `Open`.
- **In:** the schema of the writer in every Commit record, and the refusal of a newer format or schema before any change.
- **In:** one state machine in `store/internal/state` for both stores.
- **In:** the store fails closed after a failed write, sync, truncation, rename or removal.
- **In:** the fault harness over the crash model of `fsys`.
- **Out:** group commit. A commit belongs to a move or a batch of the control plane, not to a write of the application. A measurement that shows syncs limiting a pass below the bounds of the engine reverses this verdict.
- **Out:** the detection of the filesystem at `Open`. A filesystem outside the table is a configuration error.

## Alternatives considered

### Refuse every invalid tail

`Open` returns `ErrCorrupt` for every invalid record. An operator truncates the tail by hand.

Rejected because a crash during an append is an expected event under the fault model. It would stop the control plane until an operator acts. The tail rule loses a call that returned only when media damage hits the last record, and the fault model excludes media damage.

### Zero sectors as the evidence of a torn write

Segments are preallocated with zeros, as in etcd. `Open` treats an invalid record of the last segment as torn when one 512-byte sector of the record contains zeros alone.

Rejected because the test ignores the position of the record and matches zeros that the writer wrote, such as a sector of zeros in a payload. etcd's own test zeroes a sector inside the second of five records, and expects the repair to keep one record and to drop three complete ones. The sequence numbers and the search for a later valid record separate a torn tail from damage without a test of the content.

### A durable acknowledgement of each call

After the sync of a record, the store appends a second record that marks the call as returned. It syncs the second record before the call returns.

Rejected because every change then costs two syncs. A record without its acknowledgement would prove that its call never returned, so media damage to the last record could not lose a call. The second sync protects against that damage alone, and the fault model excludes media damage.

### Sync under the state lock

The state machine keeps one lock. A change writes and syncs its record under it.

Rejected because renewals and the expiry of leases then wait for the disk. The store applies the events that are due before the call that takes the lock. A renewal that waited behind a slow sync then finds its lease expired.

### Synchronous checkpoints

The store writes the snapshot under the locks. The log then remains below the threshold plus one record.

Rejected because every change then waits for the encoding and the sync of the whole state. The state grows with the scopes.

### Refuse only the scopes of a newer schema

`Open` opens a directory with a newer schema. A controller refuses to acquire each scope of that schema.

Rejected because the decoder of the older release skips the new fields of every record that it replays. Its next checkpoint rewrites the state without them.

### Processes that share one directory

Every process of a host opens the same directory and follows the log of the others.

Rejected because watches and sessions would then cross processes through the files, with a reader that polls the log and leases that every process renews in it. Separate processes also need a network transport, which sharder does not have.

### A network filesystem

The directory is on a filesystem that two or more hosts mount.

Rejected because the guarantees rest on the lock, `rename` and `fsync` semantics of a local filesystem. We have not checked any network filesystem against them.

### An embedded database

The store keeps its scopes in SQLite, bbolt or Pebble.

Rejected because each adds a dependency to the root module, and SQLite also needs cgo or a translation of its C code. The store needs an ordered log and a snapshot, and its state machine already keeps the scopes in memory.

### A copy of the state machine

`store/file` copies the scopes, sessions and elections of `store/memory`.

Rejected because the two copies of the checks of a commit would drift, and law 11 could then be true of one store and false of the other.

### One log per scope

Each scope has a separate log and snapshot.

Rejected because the store would then sync one file per scope, and one log orders every change of the process with one sync per call.

## Drawbacks

- The store of a process is a single point of failure. While its process is down, the control plane of its namespaces does not run.
- A namespace whose disk is lost recovers only with a new incarnation, over a fresh application-data namespace or behind an administrative barrier.
- Media damage to the last record of the log loses the call of that record when the call had returned.
- Each change waits for a sync, and the changes of one store wait for each other.
- After a failed write or sync, the process must open the store again.
- The disk needs room for two snapshots and the log limit: about twice the state plus 256 MiB with the defaults.
- `Open` syncs every file before it replays: one sync for the snapshot and one for each segment.
- `store/memory` moves four files of its state machine, 1,263 lines of `scope.go`, `session.go`, `election.go` and `token.go`, into `store/internal/state`.
- Every module that imports sharder downloads `golang.org/x/sys`.
- The store has two locks instead of one, and the order of the locks is a rule of its code.
- On Windows the store keeps its guarantee through the end of a process, and not through a power loss.

## Open questions

None.

## Unresolved and future work

- Replicated stores are not proposed here.
- A directory that two or more processes share, and a directory on a network filesystem, are not proposed here.

## References

| What | Where |
|---|---|
| The laws of a store and the loss of a store | RFC-0007, Laws and conformance, and Restoring a store from a backup |
| The data plane during an outage of the store | RFC-0001, Properties |
| The schema that a controller refuses | `store/store.go`, `Schema` |
| kanon, unknown fields | <https://github.com/thesm-os/kanon/blob/main/docs/rfc/0001-wire-format.md> |
| Linux, `fsync(2)` | <https://man7.org/linux/man-pages/man2/fsync.2.html> |
| Linux, the data modes of ext4 | <https://github.com/torvalds/linux/blob/master/Documentation/admin-guide/ext4.rst> |
| Pillai et al., "All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications", OSDI 2014, Table 1 and section 4 | <https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai> |
| PostgreSQL, fsync errors | <https://wiki.postgresql.org/wiki/Fsync_Errors> |
| PostgreSQL, `recovery_init_sync_method` | <https://www.postgresql.org/docs/current/runtime-config-error-handling.html> |
| PostgreSQL, WAL reliability | <https://www.postgresql.org/docs/current/wal-reliability.html> |
| etcd, the write-ahead log | <https://github.com/etcd-io/etcd/tree/main/server/storage/wal> |
| Pebble, the crash clone of `MemFS` and `errorfs` | <https://github.com/cockroachdb/pebble/tree/master/vfs> |
| Microsoft, `FlushFileBuffers` | <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers> |
| RocksDB, WAL recovery modes | <https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes> |
| RocksDB, the recyclable record format | <https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format> |
| SQLite, hardware assumptions | <https://www.sqlite.org/atomiccommit.html> |
| Go, `F_FULLFSYNC` in `os.File.Sync` on macOS | <https://github.com/golang/go/blob/go1.27.2/src/internal/poll/fd_fsync_darwin.go> |
| `golang.org/x/sys` | <https://pkg.go.dev/golang.org/x/sys> |
| Microsoft, `LockFileEx` | <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex> |
| Microsoft, `MoveFileExW` | <https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw> |
| The counters of the memory store | `store/memory/election.go`, `election.last`, and `store/memory/session.go`, `members.last` |
