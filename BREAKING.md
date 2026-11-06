# TAROT design decisions and limits

## Decisions

- One append-only WAL remains the sole authoritative database data file. The
  version 1 record format was retained; CRC32C validates each record and batch
  transaction markers preserve atomic replay.
- A successful mutation is acknowledged only after `FileChannel.force(true)`.
  The in-memory `HashMap` changes after the durable append succeeds.
- All operations take the instance lock, normalized-path JVM lock, then the
  sibling `.lock` OS `FileLock`; release is reversed. Reads are globally
  serialized to keep the index and WAL view consistent across processes.
- Compaction writes and forces a separate candidate, validates it through the
  normal recovery path, advances generation metadata, and uses atomic replace.
  The candidate is temporary; only one database data file is authoritative.

## Failure behavior

- A torn/invalid WAL suffix is ignored and truncated at the last valid committed
  boundary on recovery. An incomplete batch never becomes visible.
- If append/force fails, no success ACK is sent and no in-memory mutation is
  published. A caller that loses the response cannot know whether the operation
  committed; the Go tester retries deterministic idempotent mutations and only
  journals a matching ACK.
- If compaction fails before replacement, the original WAL remains authoritative.
  If a process stops after generation advancement, peers reload whichever WAL is
  at the authoritative path. Compaction has no non-atomic replacement fallback.
- Injected post-replacement failure returns an error even though replacement
  occurred; the next operation detects the generation change and reloads the
  compacted WAL. Tests cover this uncertain caller outcome explicitly.

## Trade-offs and known limits

- Global exclusive locking favors straightforward cross-process correctness over
  parallel reads. The JVM path registry retains entries for the JVM lifetime;
  symlink aliases may bypass normalized-path identity.
- Compaction temporarily needs space for both old and candidate WALs. It may fail
  if the configured max size or filesystem cannot accommodate the candidate.
- Process termination tests do not establish power-loss safety, directory-entry
  persistence, actual physical disk exhaustion, or behavior on every filesystem.
  `force(true)` and atomic move guarantees depend on the OS and storage stack.
- I/O failures are injected at deterministic code boundaries. The failed-force
  test does not emulate a device or operating system that loses acknowledged
  writes after power failure; no hardware flush failure was induced.
- Million-key tests and sub-two-second recovery were measured on one Windows
  machine only; they are not universal latency guarantees.
