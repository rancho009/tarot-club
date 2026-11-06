package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type settings struct {
	java          string
	classPath     string
	database      string
	oraclePath    string
	workers       int
	operations    int
	seed          int64
	crash         bool
	large         bool
	batchSize     int
	mode          string
	maxFileBytes  int64
	limitBytes    int64
	crashInterval time.Duration
}

type crashStats struct {
	mu                           sync.Mutex
	attempts                     uint64
	successfulKills              uint64
	alreadyExited                uint64
	noProcess                    uint64
	killErrors                   uint64
	acknowledgedBeforeKill       uint64
	maxAckBeforeKill             uint64
	ambiguousInFlight            uint64
	compactionsInFlight          uint64
	restartRecoveryCycles        uint64
	recoveryVerificationFailures uint64
}

func (s *crashStats) recordAttempt(outcome killOutcome, activeWrite bool,
	activeCompaction bool, acknowledged uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	switch outcome {
	case killSucceeded:
		s.successfulKills++
		s.acknowledgedBeforeKill += acknowledged
		if acknowledged > s.maxAckBeforeKill {
			s.maxAckBeforeKill = acknowledged
		}
		if activeWrite {
			s.ambiguousInFlight++
		}
		if activeCompaction {
			s.compactionsInFlight++
		}
	case killAlreadyExited:
		s.alreadyExited++
	case killNoProcess:
		s.noProcess++
	case killFailed:
		s.killErrors++
	}
}

func (s *crashStats) addRestarts(count uint64) {
	s.mu.Lock()
	s.restartRecoveryCycles += count
	s.mu.Unlock()
}

func (s *crashStats) verificationFailed() {
	s.mu.Lock()
	s.recoveryVerificationFailures++
	s.mu.Unlock()
}

func (s *crashStats) report() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("CRASH counters: attempts=%d successful-kills=%d already-exited=%d no-process=%d kill-errors=%d max-ACK-responses-before-kill=%d sum-of-ACK-snapshots-at-kills=%d ambiguous-in-flight=%d compactions-in-flight-at-kill=%d restart-recovery-cycles=%d recovery-verification-failures=%d\n",
		s.attempts, s.successfulKills, s.alreadyExited, s.noProcess, s.killErrors,
		s.maxAckBeforeKill, s.acknowledgedBeforeKill, s.ambiguousInFlight,
		s.compactionsInFlight, s.restartRecoveryCycles,
		s.recoveryVerificationFailures)
}

func main() {
	config := parseFlags()
	if err := validateSettings(config); err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}
	var err error
	if config.mode == "limit" {
		err = runLimitTest(config)
	} else if config.mode == "batch" {
		err = runLargeBatchTest(config)
	} else if config.mode == "scale" {
		err = runMillionKeyScale(config)
	} else if config.mode == "compact" {
		err = runCompactionWorkload(config)
	} else {
		err = runStress(config)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "TORTURE FAILED:", err)
		os.Exit(1)
	}
}

func parseFlags() settings {
	var config settings
	flag.StringVar(&config.java, "java", "java", "Java executable")
	flag.StringVar(&config.classPath, "classpath", "out", "compiled TAROT class directory/jar path")
	flag.StringVar(&config.database, "db", "", "database path (empty creates a new retained temp directory)")
	flag.StringVar(&config.oraclePath, "oracle", "", "ACK oracle JSONL path (default: <db>.oracle.jsonl)")
	flag.IntVar(&config.workers, "workers", 4, "concurrent Java serve processes")
	flag.IntVar(&config.operations, "ops", 1000, "logical operations per writer")
	flag.Int64Var(&config.seed, "seed", 1, "reproducible workload seed")
	flag.BoolVar(&config.crash, "crash", false, "randomly terminate Java processes and retry in-flight idempotent requests")
	flag.BoolVar(&config.large, "large", false, "include 1 MiB values and larger batches")
	flag.IntVar(&config.batchSize, "batch-size", 8, "maximum logical operations in a generated batch (1 disables batches)")
	flag.StringVar(&config.mode, "mode", "stress", "stress, batch, scale, compact, or limit")
	flag.Int64Var(&config.maxFileBytes, "max-file-bytes", 1024*1024*1024, "WAL limit for stress mode")
	flag.Int64Var(&config.limitBytes, "limit-bytes", 64*1024, "configured WAL limit used by limit mode")
	var crashIntervalMillis int
	flag.IntVar(&crashIntervalMillis, "crash-interval-ms", 100,
		"fixed delay between seeded process-kill attempts in crash mode")
	flag.Parse()
	config.crashInterval = time.Duration(crashIntervalMillis) * time.Millisecond
	return config
}

func validateSettings(config settings) error {
	if config.workers < 1 || config.workers > 64 {
		return errors.New("workers must be between 1 and 64")
	}
	if config.operations < 1 {
		return errors.New("ops must be positive")
	}
	if config.batchSize < 1 || config.batchSize > 10000 {
		return errors.New("batch-size must be between 1 and 10000")
	}
	if config.mode != "stress" && config.mode != "batch" && config.mode != "scale" &&
		config.mode != "compact" && config.mode != "limit" {
		return errors.New("mode must be stress, batch, scale, compact, or limit")
	}
	if config.maxFileBytes < 1 {
		return errors.New("max-file-bytes must be positive")
	}
	if config.limitBytes < 1 {
		return errors.New("limit-bytes must be positive")
	}
	if config.crashInterval < time.Millisecond {
		return errors.New("crash-interval-ms must be at least 1")
	}
	if config.mode == "limit" && config.limitBytes < 4096 {
		return errors.New("limit-bytes must be at least 4096 to permit a committed record")
	}
	return nil
}

func resolveDatabase(config settings) (string, string, error) {
	database := config.database
	if database == "" {
		directory, err := os.MkdirTemp("", "tarot-torture-")
		if err != nil {
			return "", "", err
		}
		database = filepath.Join(directory, "tarot.db")
	}
	database, err := filepath.Abs(database)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(filepath.Dir(database), 0755); err != nil {
		return "", "", err
	}
	oracle := config.oraclePath
	if oracle == "" {
		oracle = database + ".oracle.jsonl"
	}
	oracle, err = filepath.Abs(oracle)
	if err != nil {
		return "", "", err
	}
	return database, oracle, nil
}

func runStress(config settings) error {
	runID, err := newRunID()
	if err != nil {
		return fmt.Errorf("create unique run ID: %w", err)
	}
	database, oraclePath, err := resolveDatabase(config)
	if err != nil {
		return err
	}
	if err := validateExistingPair(database, oraclePath); err != nil {
		return err
	}
	oracle, err := openOracle(oraclePath)
	if err != nil {
		return err
	}
	defer oracle.close()

	processes := make([]*javaProcess, config.workers)
	for i := range processes {
		processes[i] = newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	}
	defer func() {
		for _, process := range processes {
			process.close()
		}
	}()

	var chaosStats crashStats
	if config.crash {
		defer chaosStats.report()
		defer func() {
			var restarts uint64
			for _, process := range processes {
				restarts += process.restartCount.Load()
			}
			chaosStats.addRestarts(restarts)
		}()
		// Start every target before enabling chaos so early attempts never
		// silently miss a process that has not been launched yet.
		for _, process := range processes {
			if err := process.start(); err != nil {
				return fmt.Errorf("prestart Java writer: %w", err)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var chaos sync.WaitGroup
	if config.crash {
		chaos.Add(1)
		go func() {
			defer chaos.Done()
			crashLoop(ctx, processes, config.seed, config.crashInterval, &chaosStats)
		}()
	}

	start := make(chan struct{})
	errCh := make(chan error, config.workers)
	var writers sync.WaitGroup
	for writerID, process := range processes {
		writers.Add(1)
		go func(writerID int, process *javaProcess) {
			defer writers.Done()
			<-start
			errCh <- runWriter(writerID, config, runID, process, oracle)
		}(writerID, process)
	}
	close(start)
	writers.Wait()
	cancel()
	chaos.Wait()
	close(errCh)
	for writerErr := range errCh {
		if writerErr != nil {
			return writerErr
		}
	}

	for _, process := range processes {
		process.close()
	}
	verifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer verifier.close()
	if _, err := verifier.check(); err != nil {
		if config.crash {
			chaosStats.verificationFailed()
		}
		return fmt.Errorf("CHECK after recovery: %w", err)
	}
	actual, err := verifier.dump()
	if err != nil {
		if config.crash {
			chaosStats.verificationFailed()
		}
		return fmt.Errorf("DUMP after recovery: %w", err)
	}
	if err := compareOracle(oracle.snapshot(), actual); err != nil {
		if config.crash {
			chaosStats.verificationFailed()
		}
		return err
	}
	fmt.Printf("PASS seed=%d writers=%d ops-per-writer=%d ACK-records=%d keys=%d\n",
		config.seed, config.workers, config.operations, oracle.ackCount(), len(actual))
	fmt.Println("database:", database)
	fmt.Println("ACK oracle:", oraclePath)
	return nil
}

func runWriter(writerID int, config settings, runID string, process *javaProcess,
	oracle *ackOracle) error {
	rng := rand.New(rand.NewSource(config.seed + int64(writerID)*0x5deece66d))
	active := make([][]byte, 0)
	keySequence := 0
	for sequence := 0; sequence < config.operations; {
		count := 1
		if config.batchSize > 1 && sequence%13 == 0 {
			count = config.batchSize
			if remaining := config.operations - sequence; count > remaining {
				count = remaining
			}
		}
		ops := make([]operation, 0, count)
		for i := 0; i < count; i++ {
			if len(active) > 0 && rng.Intn(5) == 0 {
				at := rng.Intn(len(active))
				key := active[at]
				active[at] = active[len(active)-1]
				active = active[:len(active)-1]
				ops = append(ops, operation{kind: "DEL", key: key})
				continue
			}
			key := []byte(fmt.Sprintf("seed-%d/writer-%d/key-%d", config.seed, writerID, keySequence))
			keySequence++
			size := rng.Intn(1024)
			if config.large && (sequence+i)%97 == 0 {
				size = 1 << 20
			} else if config.large && count > 1 && (sequence+i)%29 == 0 {
				size = 256 << 10
			}
			value := make([]byte, size)
			if _, err := rng.Read(value); err != nil {
				return err
			}
			active = append(active, key)
			ops = append(ops, operation{kind: "PUT", key: key, value: value})
		}
		opID := fmt.Sprintf("%s-w%d-o%d", runID, writerID, sequence)
		req := request{id: opID, ops: ops}
		if err := process.execute(req); err != nil {
			return fmt.Errorf("writer %d operation %d: %w", writerID, sequence, err)
		}
		if err := oracle.record(opID, ops); err != nil {
			return fmt.Errorf("persist ACK oracle after %s: %w", opID, err)
		}
		if len(ops) != 0 {
			probe := ops[len(ops)-1].key
			getID := fmt.Sprintf("%s-w%d-g%d", runID, writerID, sequence)
			got, found, getErr := process.get(getID, probe)
			if getErr != nil {
				return fmt.Errorf("writer %d GET after %s: %w", writerID, opID, getErr)
			}
			want, wantFound := oracle.lookup(probe)
			if found != wantFound || (found && !bytesEqual(got, want)) {
				return fmt.Errorf("writer %d GET disagrees with ACK oracle after %s", writerID, opID)
			}
		}
		sequence += count
	}
	return nil
}

func crashLoop(ctx context.Context, processes []*javaProcess, seed int64,
	interval time.Duration, stats *crashStats) {
	rng := rand.New(rand.NewSource(seed ^ 0x6a09e667f3bcc909))
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			process := processes[rng.Intn(len(processes))]
			active := process.activeWrite.Load()
			activeCompaction := process.activeCompaction.Load()
			acknowledged := uint64(0)
			for _, candidate := range processes {
				acknowledged += candidate.ackCount.Load()
			}
			outcome := process.kill()
			stats.recordAttempt(outcome, active, activeCompaction, acknowledged)
		}
	}
}

func runLimitTest(config settings) error {
	runID, err := newRunID()
	if err != nil {
		return fmt.Errorf("create unique run ID: %w", err)
	}
	base, _, err := resolveDatabase(config)
	if err != nil {
		return err
	}
	directory := base + ".limit-" + fmt.Sprint(config.seed)
	if err := os.Mkdir(directory, 0755); err != nil {
		return fmt.Errorf("create fresh limit-test directory (will not overwrite): %w", err)
	}
	database := filepath.Join(directory, "limit.db")
	oracle, err := openOracle(database + ".oracle.jsonl")
	if err != nil {
		return err
	}
	defer oracle.close()
	process := newJavaProcess(config.java, config.classPath, database, config.limitBytes)
	defer process.close()
	for i := 0; i < config.operations; i++ {
		key := []byte(fmt.Sprintf("limit/%d", i))
		value := make([]byte, 2048)
		for j := range value {
			value[j] = byte(i + j)
		}
		ops := []operation{{kind: "PUT", key: key, value: value}}
		opID := fmt.Sprintf("%s-limit-%d", runID, i)
		err := process.execute(request{id: opID, ops: ops})
		if err != nil {
			if !strings.Contains(err.Error(), "Maximum WAL file size exceeded") {
				return fmt.Errorf("unexpected size-limit error: %w", err)
			}
			process.close()
			verifier := newJavaProcess(config.java, config.classPath, database, config.limitBytes)
			defer verifier.close()
			if _, checkErr := verifier.check(); checkErr != nil {
				return fmt.Errorf("CHECK after configured limit: %w", checkErr)
			}
			actual, dumpErr := verifier.dump()
			if dumpErr != nil {
				return fmt.Errorf("reopen after configured limit: %w", dumpErr)
			}
			if compareErr := compareOracle(oracle.snapshot(), actual); compareErr != nil {
				return compareErr
			}
			fmt.Printf("PASS configured limit=%d bytes; %d ACKs recovered; database=%s\n",
				config.limitBytes, oracle.ackCount(), database)
			return nil
		}
		if err := oracle.record(opID, ops); err != nil {
			return err
		}
	}
	return errors.New("size-limit test did not reach the configured file limit")
}

func runLargeBatchTest(config settings) error {
	runID, err := newRunID()
	if err != nil {
		return fmt.Errorf("create unique run ID: %w", err)
	}
	database, oraclePath, err := resolveDatabase(config)
	if err != nil {
		return err
	}
	if err := validateExistingPair(database, oraclePath); err != nil {
		return err
	}
	oracle, err := openOracle(oraclePath)
	if err != nil {
		return err
	}
	defer oracle.close()
	process := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer process.close()
	if err := process.start(); err != nil {
		return fmt.Errorf("start Java batch process: %w", err)
	}

	seedOps := []operation{
		{kind: "PUT", key: []byte("batch-existing-update"), value: []byte("before")},
		{kind: "PUT", key: []byte("batch-existing-delete"), value: []byte("before")},
	}
	seedID := runID + "-seed"
	if err := process.execute(request{id: seedID, ops: seedOps}); err != nil {
		return fmt.Errorf("seed existing keys: %w", err)
	}
	if err := oracle.record(seedID, seedOps); err != nil {
		return fmt.Errorf("persist seed ACK oracle: %w", err)
	}

	const operationCount = 10_000
	operations := make([]operation, 0, operationCount)
	boundaryKey := bytes.Repeat([]byte{'K'}, 1024)
	boundaryValue := bytes.Repeat([]byte{0x5a}, 1<<20)
	for phase := 0; phase < 10; phase++ {
		for keyIndex := 0; keyIndex < 1000; keyIndex++ {
			var op operation
			switch {
			case phase == 0 && keyIndex == 0:
				op = operation{kind: "PUT", key: []byte("batch-existing-update"), value: []byte("after")}
			case phase == 0 && keyIndex == 1:
				op = operation{kind: "DEL", key: []byte("batch-existing-delete")}
			case phase == 9 && keyIndex == 997:
				op = operation{kind: "DEL", key: []byte("batch-existing-delete")}
			case phase == 9 && keyIndex == 998:
				op = operation{kind: "PUT", key: []byte("batch-final-empty"), value: []byte{}}
			case phase == 9 && keyIndex == 999:
				op = operation{kind: "PUT", key: boundaryKey, value: boundaryValue}
			default:
				key := []byte(fmt.Sprintf("batch-duplicate-%04d", keyIndex))
				if phase == 1 || (phase == 4 && keyIndex%4 == 0) ||
					(phase == 6 && keyIndex%5 == 0) ||
					(phase == 8 && keyIndex%2 == 0) {
					op = operation{kind: "DEL", key: key}
				} else if (phase == 3 && keyIndex%3 == 0) ||
					(phase == 7 && keyIndex%7 == 0) {
					op = operation{kind: "PUT", key: key, value: []byte{}}
				} else {
					op = operation{kind: "PUT", key: key,
						value: []byte(fmt.Sprintf("value-%d-%04d", phase, keyIndex))}
				}
			}
			operations = append(operations, op)
		}
	}
	if len(operations) != operationCount {
		return fmt.Errorf("generated %d operations; wanted %d", len(operations), operationCount)
	}

	var stats crashStats
	killDone := make(chan struct{})
	if config.crash {
		go func() {
			defer close(killDone)
			timer := time.NewTimer(config.crashInterval)
			defer timer.Stop()
			<-timer.C
			active := process.activeWrite.Load()
			activeCompaction := process.activeCompaction.Load()
			acknowledged := process.ackCount.Load()
			stats.recordAttempt(process.kill(), active, activeCompaction, acknowledged)
		}()
	}
	batchID := runID + "-batch-10000"
	started := time.Now()
	operationErr := process.execute(request{id: batchID, ops: operations})
	duration := time.Since(started)
	if config.crash {
		<-killDone
	}
	if config.crash {
		stats.addRestarts(process.restartCount.Load())
	}
	if operationErr != nil {
		if config.crash {
			stats.report()
		}
		return fmt.Errorf("10,000-operation batch did not receive matching ACK: %w", operationErr)
	}
	if err := oracle.record(batchID, operations); err != nil {
		return fmt.Errorf("persist 10,000-operation batch ACK: %w", err)
	}
	for _, probe := range [][]byte{[]byte("batch-existing-update"),
		[]byte("batch-existing-delete"), []byte("batch-final-empty"), boundaryKey} {
		got, found, getErr := process.get(runID+"-probe", probe)
		if getErr != nil {
			return fmt.Errorf("GET large-batch probe: %w", getErr)
		}
		want, wantFound := oracle.lookup(probe)
		if found != wantFound || (found && !bytes.Equal(got, want)) {
			return fmt.Errorf("GET probe disagrees with oracle for key %q", probe)
		}
	}
	process.close()
	verifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer verifier.close()
	if _, err := verifier.check(); err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return fmt.Errorf("CHECK after large-batch recovery: %w", err)
	}
	actual, err := verifier.dump()
	if err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return fmt.Errorf("DUMP after large-batch recovery: %w", err)
	}
	if err := compareOracle(oracle.snapshot(), actual); err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return err
	}
	fmt.Printf("PASS batch-operations=%d seed=%d batch-ms=%.3f ACK-records=%d keys=%d database=%s oracle=%s\n",
		len(operations), config.seed, float64(duration.Microseconds())/1000,
		oracle.ackCount(), len(actual), database, oraclePath)
	if config.crash {
		stats.report()
	}
	return nil
}

func runMillionKeyScale(config settings) error {
	const keyCount = 1_000_000
	const batchSize = 10_000
	runID, err := newRunID()
	if err != nil {
		return fmt.Errorf("create unique run ID: %w", err)
	}
	database, oraclePath, err := resolveDatabase(config)
	if err != nil {
		return err
	}
	for _, path := range []string{database, oraclePath, database + ".lock"} {
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("scale mode requires fresh paths; refusing existing file %s", path)
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
	}
	oracle, err := openScaleOracle(oraclePath, config.seed)
	if err != nil {
		return fmt.Errorf("create exclusive scale oracle: %w", err)
	}
	defer oracle.close()

	writer := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	if err := writer.start(); err != nil {
		writer.close()
		return fmt.Errorf("start scale writer: %w", err)
	}
	defer writer.close()
	var stats crashStats
	var killDone chan struct{}
	if config.crash {
		killDone = make(chan struct{})
		go func() {
			defer close(killDone)
			timer := time.NewTimer(config.crashInterval)
			defer timer.Stop()
			<-timer.C
			active := writer.activeWrite.Load()
			activeCompaction := writer.activeCompaction.Load()
			acknowledged := writer.ackCount.Load()
			stats.recordAttempt(writer.kill(), active, activeCompaction, acknowledged)
		}()
	}

	insertStart := time.Now()
	for start := 0; start < keyCount; start += batchSize {
		operations := make([]operation, batchSize)
		for offset := range operations {
			index := start + offset
			operations[offset] = operation{kind: "PUT", key: scaleKey(index),
				value: scaleValue(index, config.seed)}
		}
		id := fmt.Sprintf("%s-put-%07d", runID, start)
		if err := writer.execute(request{id: id, ops: operations}); err != nil {
			writer.close()
			if config.crash {
				<-killDone
				stats.addRestarts(writer.restartCount.Load())
				stats.report()
			}
			return fmt.Errorf("million-key PUT batch at %d: %w", start, err)
		}
		if err := oracle.record(id, operations); err != nil {
			writer.close()
			return fmt.Errorf("sync scale oracle after PUT batch %d: %w", start, err)
		}
		if (start+batchSize)%100_000 == 0 {
			fmt.Printf("scale progress inserted=%d/%d\n", start+batchSize, keyCount)
		}
	}
	insertDuration := time.Since(insertStart)
	if config.crash {
		<-killDone
		stats.addRestarts(writer.restartCount.Load())
	}
	writer.close()
	insertBytes, err := fileSize(database)
	if err != nil {
		return err
	}
	countAfterPut, putFingerprint := oracle.snapshot()
	insertVerifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer insertVerifier.close()
	verifyOpenStart := time.Now()
	if err := insertVerifier.start(); err != nil {
		return fmt.Errorf("reopen after million-key PUT: %w", err)
	}
	insertOpenDuration := time.Since(verifyOpenStart)
	checkStart := time.Now()
	checkedCount, err := insertVerifier.check()
	insertCheckDuration := time.Since(checkStart)
	if err != nil {
		return fmt.Errorf("CHECK after million-key PUT: %w", err)
	}
	actualCount, actualFingerprint, err := insertVerifier.dumpFingerprint()
	if err != nil {
		return fmt.Errorf("DUMP fingerprint after million-key PUT: %w", err)
	}
	if checkedCount != keyCount || actualCount != countAfterPut ||
		actualFingerprint != putFingerprint {
		return fmt.Errorf("million-key PUT recovery disagrees with disk oracle: CHECK=%d DUMP=%d oracle=%d",
			checkedCount, actualCount, countAfterPut)
	}
	insertVerifier.close()

	deleteProcess := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer deleteProcess.close()
	deleteOpenStart := time.Now()
	if err := deleteProcess.start(); err != nil {
		return fmt.Errorf("reopen for million-key DELETE: %w", err)
	}
	deleteOpenDuration := time.Since(deleteOpenStart)
	deleteStart := time.Now()
	for start := 0; start < keyCount; start += batchSize {
		operations := make([]operation, batchSize)
		for offset := range operations {
			operations[offset] = operation{kind: "DEL", key: scaleKey(start + offset)}
		}
		id := fmt.Sprintf("%s-del-%07d", runID, start)
		if err := deleteProcess.execute(request{id: id, ops: operations}); err != nil {
			deleteProcess.close()
			return fmt.Errorf("million-key DELETE batch at %d: %w", start, err)
		}
		if err := oracle.record(id, operations); err != nil {
			deleteProcess.close()
			return fmt.Errorf("sync scale oracle after DELETE batch %d: %w", start, err)
		}
		if (start+batchSize)%100_000 == 0 {
			fmt.Printf("scale progress deleted=%d/%d\n", start+batchSize, keyCount)
		}
	}
	deleteDuration := time.Since(deleteStart)
	deleteProcess.close()
	deletedBytes, err := fileSize(database)
	if err != nil {
		return err
	}

	finalCount, finalFingerprint := oracle.snapshot()
	finalVerifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer finalVerifier.close()
	finalOpenStart := time.Now()
	if err := finalVerifier.start(); err != nil {
		return fmt.Errorf("final reopen after million-key DELETE: %w", err)
	}
	finalOpenDuration := time.Since(finalOpenStart)
	finalCheckStart := time.Now()
	finalCheckedCount, err := finalVerifier.check()
	finalCheckDuration := time.Since(finalCheckStart)
	if err != nil {
		return fmt.Errorf("final CHECK after million-key DELETE: %w", err)
	}
	finalActualCount, finalActualFingerprint, err := finalVerifier.dumpFingerprint()
	if err != nil {
		return fmt.Errorf("final DUMP fingerprint: %w", err)
	}
	if finalCount != 0 || finalCheckedCount != 0 || finalActualCount != 0 ||
		finalFingerprint != finalActualFingerprint {
		return fmt.Errorf("million-key DELETE recovery disagrees with oracle: CHECK=%d DUMP=%d oracle=%d",
			finalCheckedCount, finalActualCount, finalCount)
	}
	compactStart := time.Now()
	if err := finalVerifier.compact(); err != nil {
		return fmt.Errorf("COMPACT after million-key DELETE: %w", err)
	}
	compactDuration := time.Since(compactStart)
	finalVerifier.close()
	compactedBytes, err := fileSize(database)
	if err != nil {
		return err
	}
	if compactedBytes >= deletedBytes {
		return fmt.Errorf("million-key compaction did not reduce WAL: before=%d after=%d",
			deletedBytes, compactedBytes)
	}
	postCompact := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer postCompact.close()
	postCompactOpenStart := time.Now()
	if err := postCompact.start(); err != nil {
		return fmt.Errorf("reopen after million-key compaction: %w", err)
	}
	postCompactOpenDuration := time.Since(postCompactOpenStart)
	postCompactCheckStart := time.Now()
	postCompactCount, err := postCompact.check()
	postCompactCheckDuration := time.Since(postCompactCheckStart)
	if err != nil {
		return fmt.Errorf("CHECK after million-key compaction: %w", err)
	}
	postCompactActualCount, postCompactFingerprint, err := postCompact.dumpFingerprint()
	if err != nil {
		return fmt.Errorf("DUMP after million-key compaction: %w", err)
	}
	if postCompactCount != 0 || postCompactActualCount != 0 ||
		postCompactFingerprint != finalFingerprint {
		return fmt.Errorf("million-key compaction recovery disagrees with oracle: CHECK=%d DUMP=%d oracle=%d",
			postCompactCount, postCompactActualCount, finalCount)
	}
	postCompact.close()

	printScaleRate("Go+Java PUT", keyCount, insertDuration)
	printScaleRate("Go+Java DELETE", keyCount, deleteDuration)
	fmt.Printf("PASS scale million-key: inserted=%d deleted=%d insert-file-bytes=%d delete-file-bytes=%d "+
		"insert-reopen-ms=%.3f insert-CHECK-ms=%.3f delete-reopen-ms=%.3f final-reopen-ms=%.3f "+
		"final-CHECK-ms=%.3f compact-file-bytes=%d compact-ms=%.3f post-compact-reopen-ms=%.3f "+
		"post-compact-CHECK-ms=%.3f oracle-resident-key-values=0 database=%s oracle=%s\n",
		keyCount, keyCount, insertBytes, deletedBytes, float64(insertOpenDuration.Microseconds())/1000,
		float64(insertCheckDuration.Microseconds())/1000,
		float64(deleteOpenDuration.Microseconds())/1000,
		float64(finalOpenDuration.Microseconds())/1000,
		float64(finalCheckDuration.Microseconds())/1000, compactedBytes,
		float64(compactDuration.Microseconds())/1000,
		float64(postCompactOpenDuration.Microseconds())/1000,
		float64(postCompactCheckDuration.Microseconds())/1000, database, oraclePath)
	if config.crash {
		stats.report()
	}
	return nil
}

func runCompactionWorkload(config settings) error {
	const keyCount = 10_000
	const batchSize = 1_000
	const updateRounds = 10
	runID, err := newRunID()
	if err != nil {
		return err
	}
	database, oraclePath, err := resolveDatabase(config)
	if err != nil {
		return err
	}
	if err := validateExistingPair(database, oraclePath); err != nil {
		return err
	}
	if _, err := os.Stat(database + ".lock"); err == nil {
		return fmt.Errorf("compaction mode refuses existing lock file %s", database+".lock")
	} else if !os.IsNotExist(err) {
		return err
	}
	oracle, err := openOracle(oraclePath)
	if err != nil {
		return err
	}
	defer oracle.close()
	writer := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer writer.close()
	if err := writer.start(); err != nil {
		return fmt.Errorf("start compaction workload writer: %w", err)
	}

	for round := 0; round < updateRounds; round++ {
		for start := 0; start < keyCount; start += batchSize {
			operations := make([]operation, batchSize)
			for offset := range operations {
				index := start + offset
				operations[offset] = operation{kind: "PUT",
					key:   []byte(fmt.Sprintf("compact/%05d", index)),
					value: []byte(fmt.Sprintf("version=%02d/key=%05d/seed=%d", round, index, config.seed))}
			}
			id := fmt.Sprintf("%s-put-r%d-%d", runID, round, start)
			if err := writer.execute(request{id: id, ops: operations}); err != nil {
				return fmt.Errorf("compaction workload update round %d batch %d: %w", round, start, err)
			}
			if err := oracle.record(id, operations); err != nil {
				return err
			}
		}
	}
	for start := 0; start < keyCount; start += batchSize {
		operations := make([]operation, 0, batchSize/2)
		for index := start; index < start+batchSize; index++ {
			if index%2 == 0 {
				operations = append(operations, operation{kind: "DEL",
					key: []byte(fmt.Sprintf("compact/%05d", index))})
			}
		}
		if len(operations) == 0 {
			continue
		}
		id := fmt.Sprintf("%s-del-%d", runID, start)
		if err := writer.execute(request{id: id, ops: operations}); err != nil {
			return fmt.Errorf("compaction workload delete batch %d: %w", start, err)
		}
		if err := oracle.record(id, operations); err != nil {
			return err
		}
	}

	beforeVerifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	if _, err := beforeVerifier.check(); err != nil {
		beforeVerifier.close()
		return fmt.Errorf("CHECK before compaction: %w", err)
	}
	beforeDump, err := beforeVerifier.dump()
	beforeVerifier.close()
	if err != nil {
		return fmt.Errorf("DUMP before compaction: %w", err)
	}
	if err := compareOracle(oracle.snapshot(), beforeDump); err != nil {
		return fmt.Errorf("oracle mismatch before compaction: %w", err)
	}
	before, err := fileSize(database)
	if err != nil {
		return err
	}

	var stats crashStats
	killDone := make(chan struct{})
	if config.crash {
		go func() {
			defer close(killDone)
			timer := time.NewTimer(config.crashInterval)
			defer timer.Stop()
			<-timer.C
			active := writer.activeWrite.Load()
			activeCompaction := writer.activeCompaction.Load()
			acknowledged := writer.ackCount.Load()
			stats.recordAttempt(writer.kill(), active, activeCompaction, acknowledged)
		}()
	}
	compactStart := time.Now()
	compactErr := writer.compact()
	compactDuration := time.Since(compactStart)
	if config.crash {
		<-killDone
		stats.addRestarts(writer.restartCount.Load())
	}
	if compactErr != nil {
		if config.crash {
			stats.report()
		}
		return fmt.Errorf("COMPACT command: %w", compactErr)
	}
	after, err := fileSize(database)
	if err != nil {
		return err
	}
	if after >= before {
		return fmt.Errorf("compaction did not reduce file size: before=%d after=%d", before, after)
	}
	writer.close()
	verifier := newJavaProcess(config.java, config.classPath, database, config.maxFileBytes)
	defer verifier.close()
	if _, err := verifier.check(); err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return fmt.Errorf("CHECK after compaction/recovery: %w", err)
	}
	actual, err := verifier.dump()
	if err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return fmt.Errorf("DUMP after compaction/recovery: %w", err)
	}
	if err := compareOracle(oracle.snapshot(), actual); err != nil {
		if config.crash {
			stats.verificationFailed()
			stats.report()
		}
		return err
	}
	fmt.Printf("PASS compaction keys-before=%d keys-after=%d file-bytes-before=%d file-bytes-after=%d compact-ms=%.3f ACK-records=%d database=%s oracle=%s\n",
		keyCount, len(actual), before, after, float64(compactDuration.Microseconds())/1000,
		oracle.ackCount(), database, oraclePath)
	if config.crash {
		stats.report()
	}
	return nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func printScaleRate(label string, count int, duration time.Duration) {
	seconds := duration.Seconds()
	fmt.Printf("%s operations=%d seconds=%.3f ops-per-second=%.1f\n",
		label, count, seconds, float64(count)/seconds)
}

func newRunID() (string, error) {
	var random [12]byte
	if _, err := crand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

func validateExistingPair(database, oracle string) error {
	_, databaseErr := os.Stat(database)
	_, oracleErr := os.Stat(oracle)
	databaseExists := databaseErr == nil
	oracleExists := oracleErr == nil
	if databaseErr != nil && !os.IsNotExist(databaseErr) {
		return databaseErr
	}
	if oracleErr != nil && !os.IsNotExist(oracleErr) {
		return oracleErr
	}
	if databaseExists != oracleExists {
		return errors.New("database and oracle must either both exist for a resume or both be new; refusing an unverifiable run")
	}
	return nil
}

func compareOracle(expected, actual map[string][]byte) error {
	for key, want := range expected {
		got, ok := actual[key]
		if !ok {
			return fmt.Errorf("ACK oracle key missing after recovery: %q", key)
		}
		if !bytesEqual(want, got) {
			return fmt.Errorf("value mismatch after recovery for key %q", key)
		}
	}
	for key := range actual {
		if _, ok := expected[key]; !ok {
			return fmt.Errorf("database contains key not represented by ACK oracle: %q", key)
		}
	}
	return nil
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
