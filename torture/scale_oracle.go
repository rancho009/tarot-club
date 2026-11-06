package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// scaleOracle retains a count and a 256-bit XOR fingerprint instead of a
// million-entry in-memory key/value map. Its JSONL journal is synced before
// its in-memory fingerprint advances.
type scaleOracle struct {
	mu          sync.Mutex
	file        *os.File
	path        string
	seed        int64
	count       uint64
	fingerprint [sha256.Size]byte
}

func openScaleOracle(path string, seed int64) (*scaleOracle, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &scaleOracle{file: file, path: path, seed: seed}, nil
}

func (o *scaleOracle) record(opID string, ops []operation) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if opID == "" || len(ops) == 0 {
		return errors.New("scale oracle requires an operation ID and non-empty batch")
	}
	var nextFingerprint = o.fingerprint
	nextCount := o.count
	record := oracleRecord{OpID: opID, Ops: make([]oracleOperation, len(ops))}
	for index, op := range ops {
		if len(op.key) == 0 || (op.kind != "PUT" && op.kind != "DEL") {
			return fmt.Errorf("invalid scale-oracle operation at index %d", index)
		}
		record.Ops[index] = oracleOperation{Kind: op.kind, Key: op.key, Value: op.value}
		var value []byte
		if op.kind == "PUT" {
			value = op.value
			nextCount++
		} else {
			var ok bool
			value, ok = scaleValueForKey(op.key, o.seed)
			if !ok || nextCount == 0 {
				return fmt.Errorf("invalid or unmatched scale DELETE for key %q", op.key)
			}
			nextCount--
		}
		xorFingerprint(&nextFingerprint, pairFingerprint(op.key, value))
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line := append(encoded, '\n')
	written, err := o.file.Write(line)
	if err != nil {
		return err
	}
	if written != len(line) {
		return io.ErrShortWrite
	}
	if err := o.file.Sync(); err != nil {
		return err
	}
	o.fingerprint = nextFingerprint
	o.count = nextCount
	return nil
}

func (o *scaleOracle) snapshot() (uint64, [sha256.Size]byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count, o.fingerprint
}

func (o *scaleOracle) close() error { return o.file.Close() }

func scaleKey(index int) []byte {
	return []byte(fmt.Sprintf("million-key-%07d", index))
}

func scaleValue(index int, seed int64) []byte {
	value := make([]byte, 16)
	binary.BigEndian.PutUint32(value[0:4], uint32(index))
	binary.BigEndian.PutUint32(value[4:8], ^uint32(index))
	binary.BigEndian.PutUint64(value[8:16], 0x5441524F54000000^uint64(index)^uint64(seed))
	return value
}

func scaleValueForKey(key []byte, seed int64) ([]byte, bool) {
	text := string(key)
	if !strings.HasPrefix(text, "million-key-") {
		return nil, false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(text, "million-key-"))
	if err != nil || index < 0 || index >= 1_000_000 {
		return nil, false
	}
	return scaleValue(index, seed), true
}

func pairFingerprint(key, value []byte) [sha256.Size]byte {
	hasher := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(key)
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], hasher.Sum(nil))
	return fingerprint
}

func xorFingerprint(target *[sha256.Size]byte, value [sha256.Size]byte) {
	for index := range target {
		target[index] ^= value[index]
	}
}
