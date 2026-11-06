# TAROT

TAROT is a Java 21 embedded key-value database backed by one append-only WAL
file. It supports byte-array keys and values, deletion, atomic batches, recovery,
and coordinated access by multiple threads and processes.

## Build

Requires a Java 21 JDK. The Go integration uses the Go standard library and was
built with Go 1.27.3 in the verification environment.

From the repository root, compile production and test sources with:

```powershell
javac -Xlint:all -Werror -d out src/main/java/kvdb/*.java src/test/java/kvdb/*.java
```

Run the main-based test suites with `java -cp out kvdb.<TestClass>`. They include
`ByteArrayKeyTest`, `DatabaseSmokeTest`, `DatabaseEdgeTest`, `DatabaseBatchTest`,
`DatabaseCrashRecoveryTest`, `DatabaseDiskFullTest`, `ProcessLockTest`,
`WALTest`, `RecordCodecTest`, `IndexTest`, `MainProtocolTest`, and
`HumanCliTest`. Additional validation mains are `ConcurrencyAndWalValidationTest`,
`TenThousandBatchStressTest`, `MillionKeyScaleTest`, `CompactionTest`, and
`FileSizeLimitAfterCompactionTest`, `CompactionFailureInjectionTest`,
`StorageFailureInjectionTest`, and `WalCorruptionMatrixTest`.

## Human-readable Java CLI

The one-shot CLI accepts ordinary command-line text and converts it to UTF-8
bytes at the CLI boundary. The database still stores byte-array keys and values.
Quote shell arguments containing spaces:

```text
java -cp out kvdb.Main open <db-path> [max-file-bytes]
java -cp out kvdb.Main put <db-path> <key> <value>
java -cp out kvdb.Main get <db-path> <key>
java -cp out kvdb.Main del <db-path> <key>
java -cp out kvdb.Main delete <db-path> <key>
java -cp out kvdb.Main batch <db-path> <batch-file>
java -cp out kvdb.Main dump <db-path>
java -cp out kvdb.Main check <db-path>
java -cp out kvdb.Main compact <db-path>
```

```text
java -cp out kvdb.Main put demo.db "Ranjeet" "Rajeev"
java -cp out kvdb.Main get demo.db "Ranjeet"
VALUE "Rajeev"
java -cp out kvdb.Main del demo.db "Ranjeet"
DELETE OK
java -cp out kvdb.Main dump demo.db
KEY: "Ranjeet" | VALUE: "Rajeev"
```

GET prints `NOT FOUND` for a missing key. Empty values print as `VALUE ""`.
Text output escapes backslash, quote, newline (`\n`), tab (`\t`), carriage
return (`\r`), and other control characters. DUMP displays valid UTF-8 as
quoted text; bytes that are not valid UTF-8 use an explicit
`base64:<standard-base64>` representation. Binary bytes are never silently
replaced with text.

## Human-readable batch files

Pass a UTF-8 text file to `batch`. Separators are literal TAB characters
(U+0009), not spaces. Spaces within keys and values are preserved. A final line
terminator is optional.

```text
PUT<TAB>Ranjeet<TAB>Rajeev
PUT<TAB>College<TAB>MITE
PUT<TAB>line\tkey<TAB>first\nsecond\rthird\\slash
PUT<TAB>empty<TAB>
DELETE<TAB>OldKey
```

In the actual file, replace each `<TAB>` marker with one tab. The supported
escapes are `\t` for tab, `\n` for newline, `\r` for carriage return, and
`\\` for a literal backslash. Unsupported or trailing escapes, malformed
rows, invalid UTF-8, keys outside 1–1024 UTF-8 bytes, values above 1 MiB,
empty files, and more than 10,000 operations are rejected. The CLI parses and
validates the complete file before calling the existing atomic batch API, so a
valid prefix is not applied when a later row is invalid. Duplicate keys apply
in file order; the final operation for a key determines its resulting value or
presence.

## Base64 compatibility and serve protocol

The previous one-shot Base64 commands remain available by their legacy
argument counts and through explicit commands:

```text
java -cp out kvdb.Main put64 <db-path> <op-id> <key-base64> <value-base64>
java -cp out kvdb.Main get64 <db-path> <op-id> <key-base64>
java -cp out kvdb.Main del64 <db-path> <op-id> <key-base64>
java -cp out kvdb.Main batch64 <db-path> <op-id> <count>  (Base64 rows on stdin)
java -cp out kvdb.Main dump64 <db-path>
```

The old positional forms `put <db> <op-id> <key-base64> <value-base64>`,
`get <db> <op-id> <key-base64>`, `del <db> <op-id> <key-base64>`, and
`batch <db> <op-id> <count>` are retained. Their argument counts distinguish
them from the shorter human forms. Base64 text passed to a human form is
treated as ordinary UTF-8 text, not decoded.

`serve` remains the internal Go protocol and retains its Base64 framing:

```text
java -cp out kvdb.Main serve <db-path> [max-file-bytes]
```

`serve` prints `READY` after opening/recovering the database, then accepts
tab-separated UTF-8 lines. Base64 fields carry arbitrary binary keys/values.
Operation IDs must match `[A-Za-z0-9_.:-]{1,128}`.

```text
PUT<TAB>op-id<TAB>key-base64<TAB>value-base64
GET<TAB>op-id<TAB>key-base64
DEL<TAB>op-id<TAB>key-base64
BATCH<TAB>op-id<TAB>count
PUT<TAB>key-base64<TAB>value-base64    (repeat count times)
DEL<TAB>key-base64                    (repeat count times)
DUMP
CHECK
COMPACT
QUIT
```

Successful PUT, DEL, and BATCH commands return exactly `ACK <op-id>` after the
database operation returns, which is after its WAL force succeeds. GET returns
`VALUE<TAB>op-id<TAB>value-base64` or `NOT_FOUND<TAB>op-id`. Errors return
`ERROR<TAB>op-id<TAB>message-base64`. Server DUMP emits Base64 `ENTRY` rows
followed by `DUMP_DONE<TAB>count`; CHECK emits `CHECK<TAB>OK<TAB>key-count`.
CHECK runs a full recovery pass and may truncate an incomplete WAL tail; it is
not a read-only forensic command. BATCH consumes exactly its declared rows
and submits them as one atomic database batch.

## Storage and guarantees

The database file is a sequence of version 1 records. Each record has a 24-byte
big-endian header and a CRC32C checksum. Standalone PUT and DELETE records are
forced with `FileChannel.force(true)` before their operation is acknowledged.
Batches use BEGIN, operation records, and COMMIT; recovery applies the batch only
after validating the commit count and transaction checksum.

Recovery scans sequentially, validates records, rebuilds the in-memory
`HashMap` index, and truncates an incomplete or uncommitted tail to the last
verified boundary. While a database is open, each operation takes the instance
thread lock, a JVM-wide lock keyed by normalized path, and an exclusive OS lock
on the sibling `.lock` file. It incrementally replays newly appended WAL records
while holding those locks. Readers also use the global exclusive lock, so this
design favors consistency over parallel read throughput.

The sibling `.lock` file is coordination state, not database content. Do not
delete it while any process may have the database open. The implementation uses
Java file locks; validate behavior on the filesystems and operating systems
where the database will run.

## Compaction

`compact <db-path>` writes the current live state to a unique same-directory
candidate WAL, forces and fully validates the candidate, increments generation
metadata in the permitted `.lock` coordination file, then atomically replaces
the authoritative database file. Open handles detect generation changes and
rebuild their in-memory indexes before operating. The temporary candidate is
not authoritative and is cleaned on a later open. If the filesystem does not
support `ATOMIC_MOVE`, compaction fails without a non-atomic fallback.

Java tests verified process termination during candidate writing, after
replacement, and after generation advancement but before replacement. This is
process-crash evidence, not a simulation of power loss or directory metadata
loss. Keep the `.lock` file while any database process may be active.

## Measured scale and limits

On the tested Windows machine, `MillionKeyScaleTest` inserted and deleted one
million keys in 100 batches of 10,000. The final full regression run measured
15.534 s insert and 14.056 s delete (64,377 / 71,145 operations per second). The WAL was
67,006,000 bytes after insertion and 114,012,000 bytes after deletion. Database
reopen took 901 ms after insertion; CHECK took 1,169 ms after insertion and
1,701 ms after deletion. Compaction after the million PUTs and million DELETEs
reduced the empty database WAL from 114,012,000 bytes to 0 in 1.410 s, and the
post-compaction reopen took 3 ms. Go integration independently repeated the
million-key workload in normal and crash modes and verified with a streamed
oracle fingerprint without retaining one million key/value pairs in its
resident map. These measurements apply to this machine and build, not every
platform.

## Configured database file-size limit

The Java API accepts a `DatabaseConfig` maximum file size; the `open` and
`serve` CLI commands accept it as the optional byte-count argument. WAL appends
that would exceed the configured maximum fail before acknowledgement. Existing
data remains readable after a rejected write or batch. The limit is checked per
append, so batches that cannot fit in full are rejected without becoming
visible. Reopening without an explicit limit uses the documented default; set
the same configuration on each open when a stable application limit is needed.
Compaction can reduce the WAL and allow subsequent writes within the configured
limit. These tests model the configured limit and are not physical disk-full
tests.

## I/O failure and WAL corruption checks

The package-private `DatabaseFaultInjector` provides deterministic fault points
to tests; production instances use a no-op implementation. Isolated tests inject
a partial WAL write, force failure, `AccessDeniedException`, and compaction
creation/write/validation/replacement failures. No successful return is counted
as an acknowledgement when an injected append or force fails. A failed force
leaves the write outcome uncertain: recovery may find the complete record if it
remained visible to the operating system, and the caller received no success.

`WalCorruptionMatrixTest` mutates disposable copies of a valid WAL. Complete
corrupt records (bad magic/length/type/CRC or invalid batch metadata) cause open
to fail without truncating the corrupt bytes. Only an incomplete trailing header
or payload is truncated to the last verified committed boundary. The source
fixture is checked byte-for-byte after the destructive-copy cases.

Run the added targeted suites after compiling:

```powershell
java -cp out kvdb.CompactionFailureInjectionTest
java -cp out kvdb.StorageFailureInjectionTest
java -cp out kvdb.WalCorruptionMatrixTest
```

`force(true)` acknowledgements, configured file-size rejection, checksummed
recovery, and process termination were exercised. Actual physical disk
exhaustion, sudden power loss, hardware flush failures, every crash instant
inside the filesystem atomic-rename call, and every filesystem's atomic
replacement semantics remain unverified. File-lock and directory metadata
durability are platform/filesystem dependent.
