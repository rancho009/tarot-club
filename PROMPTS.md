# Development prompt archive

This file was empty when the Phase 6 delivery work began. No chronological
prompt transcript was stored in the repository. The user supplied the Phase 6
to Final Delivery request as an external attachment during this session; that
attachment and the conversation transcript are the source records.

Earlier prompts are not reconstructed here because their exact full text is
not available as a repository artifact. This avoids presenting paraphrases as
verbatim prompts. Keep future prompts in chronological order here when they are
available, preserving their exact text and labeling any excerpts as excerpts.

## Verbatim prompt archive — 2026-11-06

# TAROT — Phase 6 to Final Delivery: Scale Testing, Million-Key Validation, Safe Compaction, Documentation and Demo

Repository: `C:\Projects\tarot-club`

We have completed substantial verification of concurrency, batch validation, WAL recovery, crash recovery, and the Go torture tester.

Now complete the remaining project requirements in a controlled sequence.

## IMPORTANT PROJECT RULES

1. Inspect the existing architecture and source before making changes.
2. Preserve Java 21 compatibility.
3. Do not introduce external database engines, persistence libraries, ORM frameworks, KV wrappers, or copied database implementations.
4. Do not replace the existing WAL or record format without a documented technical necessity.
5. Do not modify unrelated functionality.
6. Never delete existing databases, test data, or oracle files.
7. Use isolated temporary database paths for all stress and crash tests.
8. Do not commit or push.
9. Do not claim tests passed unless they were actually executed.
10. Record exact commands, test outputs, timings, and limitations.
11. Preserve all existing tests and regression coverage.
12. Perform the work in the stages below. Do not skip ahead if an earlier stage exposes a correctness defect.

---

# STAGE 1 — 10,000-OPERATION BATCH STRESS TEST

### Java responsibilities

Use the actual public Java database API.

Test a batch containing exactly 10,000 operations.

Include:
- PUT operations.
- DELETE operations.
- Duplicate keys.
- Empty values.
- Updates to existing keys.
- Mixed PUT and DELETE operations.
- Boundary-sized keys and values where practical.

Verify:
- The complete batch is atomic.
- Duplicate operations follow the documented order.
- No partial batch becomes visible.
- Acknowledged batches survive database reopen.
- Invalid batches are rejected without partial persistence.
- The database remains usable after rejection.

Measure:
- Batch execution time.
- WAL size before and after.
- Database file size.
- Recovery/open time.
- Memory usage if measurable.

### Go responsibilities

Extend the existing Go torture tester to generate and verify large batch workloads.

Requirements:
- Generate deterministic 10,000-operation batches.
- Maintain the existing acknowledged-operation oracle.
- Record only operations with matching durable acknowledgements.
- Reopen/recover the database and compare its contents against the oracle.
- Include duplicate-key and mixed-operation cases.
- Add crash testing around large batch execution where practical.
- Never treat an ambiguous in-flight batch as acknowledged.

Do not weaken existing oracle validation.

---

# STAGE 2 — MILLION-KEY TESTING

The acceptance target is at least 1,000,000 keys.

### Java responsibilities

Create an isolated scale-test harness using the real database API.

Run:

1. Insert 1,000,000 unique keys.
2. Verify representative keys throughout the key space.
3. Verify key count and final state using supported database functionality.
4. Close and reopen the database.
5. Verify persistence after reopen.
6. Delete 1,000,000 keys.
7. Verify deleted keys are absent.
8. Reopen again and verify the final state.

Use bounded memory and avoid loading the entire database into a second in-memory structure unnecessarily.

Measure:
- Total insertion time.
- Total deletion time.
- Operations per second.
- Database file size at each stage.
- JVM heap usage.
- Database open time.
- Recovery time.
- Any failures or resource exhaustion.

Do not silently skip keys or reduce the workload.

If the current implementation cannot support one million keys within available resources, identify the bottleneck and report it before proposing a targeted fix.

### Go responsibilities

Use Go as an independent workload generator and verifier.

Requirements:
- Generate deterministic million-key workloads.
- Track acknowledged operations in a scalable oracle format.
- Avoid loading all expected key/value pairs into RAM.
- Verify the final recovered state against the oracle.
- Support resumable or clearly restartable test runs if practical.
- Include normal and crash-enabled runs where resource limits permit.

Do not assume Java's own test result is sufficient verification.

---

# STAGE 3 — SAFE COMPACTION

This is a critical acceptance requirement.

The database must shrink after a workload containing approximately one million PUTs followed by one million DELETEs.

First inspect:
- WAL structure.
- Recovery rules.
- File locking.
- JVM-wide path locks.
- OS file locking.
- Open channel lifecycle.
- Database index representation.
- Current close/reopen behavior.
- Windows file replacement behavior.

### Design before implementation

Write a short technical design explaining:

1. How a consistent database snapshot is obtained.
2. How concurrent writers are prevented or coordinated.
3. How a compacted file is generated.
4. How it is validated before replacement.
5. How the old file is preserved until replacement is safe.
6. How crash recovery behaves at every replacement stage.
7. How all live database instances refresh their file channels and indexes.
8. How the design behaves on Windows, where open-file replacement may fail.
9. How temporary files are cleaned up safely.
10. How exactly one authoritative persistent data file is maintained.

Do not assume atomic rename alone makes compaction crash-safe.

### Implementation requirements

Implement compaction only after the design is reviewed against the existing locking and recovery model.

Requirements:
- Preserve every acknowledged live key/value pair.
- Discard deleted and superseded records.
- Produce a valid recoverable database file.
- Keep the authoritative data file consistent through crashes.
- Ensure live instances cannot continue writing to a stale file handle.
- Ensure failed compaction leaves the original database readable.
- Ensure recovery can distinguish an incomplete compaction from a committed replacement.
- Preserve the one-authoritative-data-file requirement.
- Do not acknowledge writes before durable persistence.

### Java tests

Test:
- Empty database compaction.
- Compaction with live records.
- Compaction with overwritten records.
- Compaction with deleted records.
- Reopen after compaction.
- File-size reduction.
- Compaction failure before replacement.
- Process termination during compaction.
- Concurrent access and compaction coordination.
- Windows-specific file replacement behavior.

### Go tests

Extend the torture tester to:
- Generate PUT/UPDATE/DELETE workloads.
- Verify the acknowledged oracle before compaction.
- Trigger or coordinate compaction through a supported Java interface.
- Crash the process at available compaction checkpoints.
- Restart and recover.
- Compare the complete recovered state against the oracle.
- Verify no acknowledged data is lost.

Do not create a second independent Go implementation of the database or compaction algorithm.

If safe compaction cannot be completed reliably, report the exact blocker. Do not ship a risky implementation just to mark the feature complete.

## Verbatim prompt archive — 2026-11-07

# TAROT – Complete the Four Remaining Phase 7 Tasks

Act as a senior Java 21 database engineer. Your task is to finish the four specific incomplete tasks in the existing TAROT project.

## Project context

TAROT is an existing Java 21 crash-safe embedded key-value database with:

- CRUD operations
- In-memory index
- Write-Ahead Log (WAL)
- CRC32C validation
- Crash recovery
- Atomic batches
- Thread and process locking
- Safe compaction
- Configurable file-size limits
- Independent Go torture tester

The core database is already implemented. Do not rebuild it.

## Your exact scope

Complete ONLY these four tasks:

### 1. Compaction failure testing

Implement and execute tests that verify existing compaction behaviour when failures occur.

Test:

- Failure during candidate-file creation.
- Failure during candidate-file writing.
- Failure during candidate validation.
- Failure before file replacement.
- Failure during or after file replacement.
- Recovery after interruption.
- Temporary-file cleanup.
- Preservation of live records.
- Prevention of deleted-record resurrection.

Use the existing compaction implementation. Fix bugs only when the tests reveal them.

### 2. Disk and filesystem failure testing

Implement and execute controlled tests for:

- WAL write failures.
- Partial writes.
- Flush or sync failures, where applicable.
- Permission-denied errors.
- Candidate-file write failures.
- File replacement failures.
- Configured file-size-limit behaviour.

Verify that failed operations are not falsely acknowledged and that existing valid data is preserved according to the database's actual durability guarantees.

Use controlled failure injection or isolated temporary directories. Do not physically fill the developer's disk.

### 3. Documentation completion

Inspect and finish the existing documentation files:

- README.md
- ARCHITECTURE.md
- BREAKING.md
- DEMO.md
- PROMPTS.md
- torture/README.md

Document only the implementation that actually exists.

Correct incomplete or outdated instructions. Include verified build, test, CLI, benchmark and Go tester commands where applicable.

Do not invent functionality or claim unsupported durability guarantees.

### 4. WAL corruption testing

Implement and execute tests for:

- Invalid WAL header or magic.
- Invalid record length.
- CRC32C mismatch.
- Truncated header.
- Truncated payload.
- Invalid operation type.
- Invalid batch metadata.
- Corruption at the beginning, middle and end of the WAL.

Verify that incomplete trailing records are handled according to the existing recovery protocol and that corruption of previously valid records is not silently ignored.

Preserve original database files during destructive corruption tests by using copies.

Fix only the bugs revealed by these tests.

---

## Strict instructions

1. First inspect the existing source code and tests.
2. Reuse the existing architecture and testing conventions.
3. Do not add new database features.
4. Do not redesign TAROT.
5. Do not rewrite the WAL format unless an existing correctness defect makes it unavoidable.
6. Do not add unrelated functionality.
7. Do not remove or weaken existing tests.
8. Do not modify demo.db or demo.db.lock.
9. Do not modify working features unnecessarily.
10. Do not commit or push changes.
11. Do not claim a test passed unless you actually ran it.
12. Maintain Java 21 compatibility and the project's existing compilation requirements.

## Execution

Complete the work in this order:

1. Inspect the implementation and identify exactly what is missing.
2. Complete compaction failure tests.
3. Complete disk/filesystem failure tests.
4. Complete WAL corruption tests.
5. Finish the existing documentation.
6. Run all existing Java tests.
7. Run all newly added Java tests.
8. Run relevant existing Go normal, crash and recovery tests.
9. Verify that no existing functionality has regressed.

Do not stop after creating a plan. Implement and test the work.

## Final report

Report only:

1. Which of the four tasks are completed.
2. Files modified or created.
3. Tests added.
4. Exact commands executed.
5. Actual PASS/FAIL results.
6. Bugs discovered and fixed.
7. Remaining incomplete work, if any.

If any task cannot be completed, explain exactly what is blocking it.

**Definition of done:** All four specified tasks have been implemented, tested and documented to the extent supported by the existing TAROT architecture. No unrelated features or enhancements are included.

---

# STAGE 4 — CONFIGURABLE FILE-SIZE LIMIT

Inspect the existing max-file-size implementation and its relationship to WAL writes and batch transactions.

Verify:
- Writes fail cleanly when the configured limit is reached.
- Previously acknowledged data remains readable.
- Recovery succeeds after rejected writes.
- Failed batches are all-or-nothing.
- File-size accounting is correct.
- Reopening preserves the configured behavior as documented.
- Compaction can reduce the file size and permit future writes where supported.

Test with small deterministic limits first, then a larger workload.

Do not simulate disk exhaustion by damaging existing project files.

---

# STAGE 5 — FINAL REGRESSION

After implementation:

Compile:

`javac -Xlint:all -Werror -d out src\main\java\kvdb\*.java src\test\java\kvdb\*.java`

Run all existing Java test mains, including:
- ByteArrayKeyTest
- DatabaseSmokeTest
- DatabaseEdgeTest
- DatabaseBatchTest
- DatabaseCrashRecoveryTest
- DatabaseDiskFullTest
- ProcessLockTest
- WALTest
- RecordCodecTest
- IndexTest
- MainProtocolTest
- HumanCliTest
- ConcurrencyAndWalValidationTest

Rebuild the Go tester:

`go build -o tarot-torture.exe .`

Run:
- Existing normal workload.
- Existing crash workload.
- 10,000-operation batch workload.
- Million-key workload where resources permit.
- Compaction verification workload.
- Crash-during-compaction workload where supported.

Keep test data isolated.

---

# STAGE 6 — FINAL DOCUMENTATION

Inspect existing documentation and update it to match the actual implementation.

Required deliverables:

### README.md
Include:
- Project overview.
- Architecture.
- Java 21 requirements.
- Build instructions.
- CLI commands.
- Human-readable CLI examples.
- Legacy Base64 interface where applicable.
- Batch file format.
- Database and WAL structure.
- Durability guarantees.
- Recovery behavior.
- Concurrency model.
- File-size limit.
- Compaction behavior and limitations.
- Test instructions.

### ARCHITECTURE.md
Include:
- System architecture diagram.
- Write path.
- Read path.
- Batch transaction path.
- WAL record structure.
- Recovery flow.
- Locking model.
- Compaction protocol.
- Failure and recovery scenarios.
- Evidence table distinguishing tested, partially tested, and untested behavior.

### BREAKING.md
Keep within two pages.

Explain:
- Important design decisions.
- Failure cases.
- Crash-recovery guarantees.
- Trade-offs.
- Known limitations.

### PROMPTS.md
Preserve the actual chronological AI prompts used during development. Do not fabricate earlier prompts or claim reconstructed prompts are verbatim.

### Go torture tester README
Document:
- Build and run instructions.
- Workload parameters.
- Oracle format.
- Crash testing.
- Batch stress mode.
- Million-key mode.
- Compaction verification.
- Counter meanings.

---

# STAGE 7 — FINAL DEMONSTRATION

Prepare a concise demonstration plan that proves:

1. PUT and GET.
2. DELETE.
3. Persistence after restart.
4. Atomic batch execution.
5. Concurrent writers.
6. Crash recovery.
7. 10,000-operation batch.
8. Million-key workload.
9. Compaction and file-size reduction.
10. Recovery after compaction.

Include exact commands and expected output based on actual test results.

Prepare a short technical explanation of:
- Why the WAL is necessary.
- How CRC32C detects corruption.
- How fsync-before-ACK protects acknowledged writes.
- How batch BEGIN/COMMIT provides atomicity.
- How recovery handles incomplete transactions.
- How compaction preserves acknowledged data.

Do not claim a demonstration result that has not been tested.

---

# REQUIRED FINAL REPORT

Provide a milestone table:

| Requirement | Result | Evidence |
|---|---|---|
| 10,000-operation batch | PASS/FAIL/NOT TESTED | Exact operation count and timings |
| 1-million-key insertion | PASS/FAIL/NOT TESTED | Actual key count and timings |
| 1-million-key deletion | PASS/FAIL/NOT TESTED | Actual deletion count and timings |
| Persistence/recovery at scale | PASS/FAIL/NOT TESTED | Reopen and recovery evidence |
| Safe compaction | PASS/FAIL/NOT TESTED | Crash and consistency evidence |
| File-size reduction | PASS/FAIL/NOT TESTED | Before/after measurements |
| File-size limit | PASS/FAIL/NOT TESTED | Rejection and recovery evidence |
| Java regression | PASS/FAIL | Exact test count |
| Go regression | PASS/FAIL | Exact outputs |
| Documentation | COMPLETE/PARTIAL | Files updated |
| Demo | READY/NOT READY | Exact reproducible commands |

Also provide:
- Every changed file.
- Every source-code fix and its justification.
- Exact commands executed.
- Actual timing and resource measurements.
- Failed attempts and limitations.
- Any untested crash windows.
- Confirmation that existing databases and oracle files were untouched.
- Confirmation that no commit or push was performed.

## EXECUTION PRIORITY

Work in this order:

1. 10,000-operation batch stress.
2. Million-key testing.
3. Safe compaction design and implementation.
4. File-size-limit validation.
5. Full regression.
6. Documentation.
7. Demo preparation.

Do not rush compaction. Data integrity takes priority over speed.

Do not stop after writing a plan. Execute the stages that can be completed safely, report actual evidence, and clearly identify any remaining blockers.
