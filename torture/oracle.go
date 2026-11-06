package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type oracleOperation struct {
	Kind  string `json:"kind"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type oracleRecord struct {
	OpID string            `json:"op_id"`
	Ops  []oracleOperation `json:"ops"`
}

type ackOracle struct {
	mu     sync.Mutex
	file   *os.File
	model  map[string][]byte
	acks   uint64
	path   string
}

func openOracle(path string) (*ackOracle, error) {
	model := make(map[string][]byte)
	seenIDs := make(map[string]struct{})
	var ackCount uint64
	readFile, err := os.Open(path)
	if err == nil {
		reader := bufio.NewReader(readFile)
		for {
			line, readErr := reader.ReadBytes('\n')
			if len(line) != 0 {
				var record oracleRecord
				if err := json.Unmarshal(line, &record); err != nil {
					_ = readFile.Close()
					return nil, fmt.Errorf("invalid oracle journal line: %w", err)
				}
				if record.OpID == "" || len(record.Ops) == 0 {
					_ = readFile.Close()
					return nil, errors.New("invalid oracle journal record: missing operation ID or operations")
				}
				if _, exists := seenIDs[record.OpID]; exists {
					_ = readFile.Close()
					return nil, fmt.Errorf("duplicate operation ID in oracle journal: %s", record.OpID)
				}
				for _, op := range record.Ops {
					if (op.Kind != "PUT" && op.Kind != "DEL") || len(op.Key) == 0 ||
						(op.Kind == "DEL" && op.Value != nil) {
						_ = readFile.Close()
						return nil, fmt.Errorf("invalid operation in oracle journal record %s", record.OpID)
					}
				}
				seenIDs[record.OpID] = struct{}{}
				applyOracle(model, record.Ops)
				ackCount++
			}
			if readErr == io.EOF {
				if len(line) != 0 {
					_ = readFile.Close()
					return nil, errors.New("oracle journal ends with an incomplete record")
				}
				break
			}
			if readErr != nil {
				_ = readFile.Close()
				return nil, readErr
			}
		}
		if err := readFile.Close(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &ackOracle{file: file, model: model, acks: ackCount, path: path}, nil
}

// record is called only after the corresponding Java ACK has been received.
func (o *ackOracle) record(opID string, ops []operation) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := oracleRecord{OpID: opID, Ops: make([]oracleOperation, len(ops))}
	for i, op := range ops {
		record.Ops[i] = oracleOperation{Kind: op.kind, Key: op.key, Value: op.value}
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
	applyOracle(o.model, record.Ops)
	o.acks++
	return nil
}

func applyOracle(model map[string][]byte, ops []oracleOperation) {
	for _, op := range ops {
		if op.Kind == "DEL" {
			delete(model, string(op.Key))
		} else {
			model[string(op.Key)] = append([]byte(nil), op.Value...)
		}
	}
}

func (o *ackOracle) snapshot() map[string][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	copyOfModel := make(map[string][]byte, len(o.model))
	for key, value := range o.model {
		copyOfModel[key] = append([]byte(nil), value...)
	}
	return copyOfModel
}

func (o *ackOracle) ackCount() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.acks
}

func (o *ackOracle) lookup(key []byte) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	value, found := o.model[string(key)]
	return append([]byte(nil), value...), found
}

func (o *ackOracle) close() error {
	return o.file.Close()
}
