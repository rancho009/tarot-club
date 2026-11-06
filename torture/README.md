# TAROT Go torture tester

This standard-library-only Go module drives the Java `kvdb.Main serve`
protocol documented in the repository README. It launches one Java serve JVM
per writer, so writes from separate processes contend on TAROT's shared database
locks.

The tester journals every successful Java ACK as JSON Lines in a separate
`<database>.oracle.jsonl` file and calls `Sync` before applying the operation to
its independent in-memory model. At the end it restarts Java, runs CHECK and
DUMP, and compares the exact recovered contents with the oracle. Existing
database/oracle pairs may be resumed together. If exactly one exists, the tester
refuses to start because it cannot establish a trustworthy baseline.

Each run uses a cryptographically random operation-ID namespace, while the seed
continues to determine generated keys, values, and crash timing. After each
acknowledged mutation or batch, the writer reads back its last touched key and
compares the result with the synchronized oracle. Oracle recovery rejects
duplicate operation IDs, malformed records, and incomplete final JSONL lines.

With `-crash`, the tester starts each Java writer before enabling a seeded
process-selection loop. It attempts kills at a fixed interval (100 ms by
default), including during commands, and retries the same operation ID and
payload after restart.
The generated PUT/DEL/BATCH requests are idempotent, so a request that committed
but lost its ACK can safely be retried before its ACK is entered in the oracle.
The report distinguishes successful kills (Kill succeeded and Wait confirmed
the process exited), already-exited targets, missing process handles, and kill
errors. It also reports matching ACK responses observed before kills, requests
in flight at a successful kill, writer restarts, and final recovery comparison
failures. This exercises process termination and recovery; it does not simulate
power loss or physical storage failure.

`-large` includes 1 MiB values and larger generated batches. `-mode batch`
submits exactly 10,000 atomic operations with duplicate keys, updates, deletes,
empty values, a 1 MiB value and a 1024-byte key, then verifies after restart.
`-mode scale` inserts and deletes one million deterministic keys in batches of
10,000, compacts the post-delete WAL, then reopens and verifies the compacted
empty state. Its synced oracle uses a streamed fingerprint without retaining
one million pairs in Go memory. `-mode compact` creates overwrite/delete
garbage, checks it against the ACK oracle, compacts, then verifies WAL shrink
and recovered contents. All three modes accept `-crash`. `-mode limit`
uses Java's configured maximum WAL size to test a rejected append, restarts the
database, and checks all ACKed data. This is a configured size-limit test, not
physical disk exhaustion.

## Windows CMD build and run

Run these commands from Command Prompt after installing Java 21 and Go:

```bat
cd /d C:\Projects\tarot-club
javac -Xlint:all -Werror -d out src\main\java\kvdb\*.java src\test\java\kvdb\*.java
cd /d C:\Projects\tarot-club\torture
go build -o tarot-torture.exe .
tarot-torture.exe -classpath ..\out -workers 4 -ops 2000 -seed 42 -crash
tarot-torture.exe -classpath ..\out -workers 4 -ops 500 -seed 43 -crash -large -batch-size 128
tarot-torture.exe -classpath ..\out -mode limit -seed 44 -limit-bytes 65536
tarot-torture.exe -classpath ..\out -mode batch -seed 45 -crash
tarot-torture.exe -classpath ..\out -mode scale -seed 46 -crash
tarot-torture.exe -classpath ..\out -mode compact -seed 47 -crash
```

The stress run creates a new database under the system temporary directory if
`-db` is omitted; it prints and retains that path and the ACK journal. To choose
paths explicitly, pass `-db C:\path\tarot.db` and optionally
`-oracle C:\path\oracle.jsonl`. The tester never deletes the database, oracle,
or limit-test directory. The limit mode creates a new sibling directory and
fails instead of reusing one with the same seed.

Useful flags:

| Flag | Purpose |
| --- | --- |
| `-java` | Java executable, default `java` |
| `-classpath` | Compiled Java classes/jars, default `out` |
| `-workers` | Concurrent Java writer processes, default 4 |
| `-ops` | Logical operations per writer, default 1000 |
| `-seed` | Reproducible generator/chaos seed |
| `-crash` | Randomly kill Java processes and retry in-flight idempotent requests |
| `-crash-interval-ms` | Fixed interval between seeded kill-target selections, default 100 ms |
| `-large` | Include large values and larger batches |
| `-batch-size` | Maximum generated batch operations, default 8; max 10000 |
| `-mode limit` | Exercise the configured WAL size limit |
| `-mode batch` | Exercise one 10,000-operation atomic batch |
| `-mode scale` | Insert/delete one million keys with bounded oracle memory |
| `-mode compact` | Exercise compaction and recovery against the oracle |
| `-limit-bytes` | WAL limit for limit mode, default 65536 |
| `-max-file-bytes` | WAL limit for stress mode, default 1 GiB |

Java-side protocol coverage is provided by `kvdb.MainProtocolTest`.

Latest scale runs: normal Go+Java inserted/deleted 1,000,000 keys in 16.964/16.511
seconds (58,947/60,565 ops/s); crash-enabled mode took 11.770/15.573 seconds
(84,962/64,216 ops/s). Both runs compacted the post-delete 114,012,000-byte WAL
to 0, then reopened and checked the compacted file. Normal/crash compaction took
8.063/8.079 seconds; final post-compaction reopen/CHECK took 132/7 ms and
198/6 ms. Both reported `oracle-resident-key-values=0`. The crash run killed
Java once with an ambiguous in-flight operation, restarted it, and reported
`recovery-verification-failures=0`. These tests do not simulate power loss or
physical disk exhaustion. Temporary databases and oracle journals are retained;
use fresh paths for independent runs.
