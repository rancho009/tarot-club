# TAROT demonstration plan

Run from `C:\Projects\tarot-club` in Windows Command Prompt. Each demo uses a
new database path. Keep the resulting files to inspect them; do not point these
commands at an existing database or oracle.

## Prepare

```bat
javac -Xlint:all -Werror -d out src\main\java\kvdb\*.java src\test\java\kvdb\*.java
cd /d C:\Projects\tarot-club\torture
"C:\Program Files\Go\bin\go.exe" build -o tarot-torture.exe .
set DEMO=%TEMP%\tarot-demo-%RANDOM%-%RANDOM%
mkdir "%DEMO%"
```

If `mkdir` says the directory already exists, choose a new `%RANDOM%` suffix.

## CRUD, restart, batch, concurrency, crash

From repository root:

```bat
cd /d C:\Projects\tarot-club
java -cp out kvdb.Main put "%DEMO%\crud.db" alpha one
java -cp out kvdb.Main get "%DEMO%\crud.db" alpha
java -cp out kvdb.Main get "%DEMO%\crud.db" alpha
java -cp out kvdb.Main del "%DEMO%\crud.db" alpha
java -cp out kvdb.Main get "%DEMO%\crud.db" alpha
```

Expected meaningful outputs are `VALUE "one"` before delete and `NOT FOUND`
after delete. Each one-shot invocation closes the database; the second GET
demonstrates persistence across process restart.

Create a tab-separated batch file with PUT and DELETE rows, then run:

```bat
java -cp out kvdb.Main batch "%DEMO%\crud.db" "%DEMO%\batch.txt"
java -cp out kvdb.Main dump "%DEMO%\crud.db"
```

The batch reports success only after the atomic database batch returns. For
concurrent and crash recovery, from `torture` run:

```bat
tarot-torture.exe -classpath ..\out -workers 4 -ops 500 -seed 42 -db "%DEMO%\stress.db" -oracle "%DEMO%\stress.oracle.jsonl"
tarot-torture.exe -classpath ..\out -workers 4 -ops 500 -seed 43 -crash -db "%DEMO%\crash.db" -oracle "%DEMO%\crash.oracle.jsonl"
```

Expected output begins `PASS`; crash output reports kill/restart counters and
`recovery-verification-failures=0`. Timing and counts vary by machine/seed.

## Large batch, scale, compaction

From `torture`:

```bat
tarot-torture.exe -classpath ..\out -mode batch -seed 45 -db "%DEMO%\batch10k.db" -oracle "%DEMO%\batch10k.oracle.jsonl"
tarot-torture.exe -classpath ..\out -mode scale -seed 46 -db "%DEMO%\million.db" -oracle "%DEMO%\million.oracle.jsonl"
tarot-torture.exe -classpath ..\out -mode compact -seed 47 -crash -db "%DEMO%\compact.db" -oracle "%DEMO%\compact.oracle.jsonl"
```

Expected batch output includes `PASS batch-operations=10000`. Scale output
includes `PASS scale million-key: inserted=1000000 deleted=1000000`,
`delete-file-bytes=114012000 compact-file-bytes=0`, and
`oracle-resident-key-values=0`. Compaction mode output includes `PASS compaction`,
lower `file-bytes-after` than `file-bytes-before`, and after crash mode a counter
line with `recovery-verification-failures=0`. Measurements include a Java live
record compaction shrinking 3,970,180 bytes to 124,010 bytes and a million-key
post-delete compaction shrinking 114,012,000 bytes to 0. The Go garbage-heavy
compaction workload shrank 7,711,600 bytes to 375,000 bytes. Actual demo timing
and kill location are nondeterministic.

## What to explain

- The WAL makes each accepted mutation recoverable by replay after restart.
- CRC32C detects a damaged or incomplete record before applying it.
- TAROT forces the WAL before returning a successful mutation ACK.
- Batch BEGIN/COMMIT markers let recovery expose the entire batch or none of it.
- Recovery scans sequentially and truncates an invalid suffix at the last
  committed boundary.
- Compaction snapshots under the exclusive path lock, forces and validates a
  candidate, advances generation metadata, and atomically replaces the WAL;
  other open instances detect the new generation and rebuild their indexes.

These commands demonstrate tested behavior on the current Windows environment.
They do not prove sudden power-loss behavior, actual disk exhaustion, or atomic
replacement guarantees on all filesystems.

## Failure and corruption checks

After compiling, run the deterministic failure suites:

```bat
cd /d C:\Projects\tarot-club
java -cp out kvdb.CompactionFailureInjectionTest
java -cp out kvdb.StorageFailureInjectionTest
java -cp out kvdb.WalCorruptionMatrixTest
```

Expected final lines are `ALL COMPACTION FAILURE INJECTION TESTS PASSED`,
`ALL STORAGE FAILURE INJECTION TESTS PASSED`, and
`ALL WAL CORRUPTION MATRIX TESTS PASSED`. Corruption tests alter only copied,
temporary WALs and verify the source fixture remains byte-for-byte unchanged.
