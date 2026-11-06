package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type operation struct {
	kind  string
	key   []byte
	value []byte
}

type request struct {
	id  string
	ops []operation
}

type javaProcess struct {
	java      string
	classPath string
	database  string
	maxBytes  int64

	mu               sync.Mutex
	procMu           sync.Mutex
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	stdout           *bufio.Reader
	stderr           *bytes.Buffer
	closed           bool
	activeWrite      atomic.Bool
	activeCompaction atomic.Bool
	restartCount     atomic.Uint64
	ackCount         atomic.Uint64
	starts           uint64 // Protected by mu; counts successful launches.
}

type killOutcome uint8

const (
	killNoProcess killOutcome = iota
	killSucceeded
	killAlreadyExited
	killFailed
)

func newJavaProcess(java, classPath, database string, maxBytes int64) *javaProcess {
	return &javaProcess{java: java, classPath: classPath, database: database, maxBytes: maxBytes}
}

func (p *javaProcess) startLocked() error {
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	args := []string{"-cp", p.classPath, "kvdb.Main", "serve", p.database}
	if p.maxBytes > 0 {
		args = append(args, strconv.FormatInt(p.maxBytes, 10))
	}
	cmd := exec.Command(p.java, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start Java serve process: %w", err)
	}
	p.procMu.Lock()
	p.cmd = cmd
	p.procMu.Unlock()
	if p.starts > 0 {
		p.restartCount.Add(1)
	}
	p.starts++
	p.stdin = stdin
	p.stdout = bufio.NewReader(stdout)
	p.stderr = stderr
	line, err := p.readLineLocked()
	if err != nil {
		p.stopLocked()
		return fmt.Errorf("waiting for Java READY: %w", err)
	}
	if line != "READY" {
		text := line
		if p.stderr.Len() > 0 {
			text += ": " + p.stderr.String()
		}
		p.stopLocked()
		return fmt.Errorf("Java server did not become ready: %s", text)
	}
	return nil
}

func (p *javaProcess) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("Java process wrapper is closed")
	}
	if p.cmd != nil {
		return nil
	}
	return p.startLocked()
}

func (p *javaProcess) execute(req request) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeWrite.Store(true)
	defer p.activeWrite.Store(false)
	if p.closed {
		return errors.New("Java process wrapper is closed")
	}
	for attempt := 0; attempt < 10; attempt++ {
		if p.cmd == nil {
			if err := p.startLocked(); err != nil {
				time.Sleep(time.Duration(attempt+1) * 25 * time.Millisecond)
				continue
			}
		}
		if err := p.writeRequestLocked(req); err != nil {
			p.stopLocked()
			continue
		}
		line, err := p.readLineLocked()
		if err != nil {
			p.stopLocked()
			continue
		}
		if line == "ACK "+req.id {
			p.ackCount.Add(1)
			return nil
		}
		if strings.HasPrefix(line, "ERROR\t") {
			return parseRemoteError(line)
		}
		return fmt.Errorf("unexpected serve response %q", line)
	}
	return errors.New("Java process repeatedly terminated before ACK")
}

func (p *javaProcess) get(opID string, key []byte) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, errors.New("Java process wrapper is closed")
	}
	for attempt := 0; attempt < 10; attempt++ {
		if p.cmd == nil {
			if err := p.startLocked(); err != nil {
				time.Sleep(time.Duration(attempt+1) * 25 * time.Millisecond)
				continue
			}
		}
		if _, err := fmt.Fprintf(p.stdin, "GET\t%s\t%s\n", opID,
			base64.StdEncoding.EncodeToString(key)); err != nil {
			p.stopLocked()
			continue
		}
		line, err := p.readLineLocked()
		if err != nil {
			p.stopLocked()
			continue
		}
		if line == "NOT_FOUND\t"+opID {
			return nil, false, nil
		}
		fields := strings.Split(line, "\t")
		if len(fields) == 3 && fields[0] == "VALUE" && fields[1] == opID {
			value, decodeErr := base64.StdEncoding.DecodeString(fields[2])
			if decodeErr != nil {
				return nil, false, fmt.Errorf("malformed GET value response: %w", decodeErr)
			}
			return value, true, nil
		}
		if strings.HasPrefix(line, "ERROR\t") {
			return nil, false, parseRemoteError(line)
		}
		return nil, false, fmt.Errorf("unexpected GET response %q", line)
	}
	return nil, false, errors.New("Java process repeatedly terminated before GET response")
}

func (p *javaProcess) writeRequestLocked(req request) error {
	if len(req.ops) == 1 && req.ops[0].kind != "BATCH" {
		op := req.ops[0]
		switch op.kind {
		case "PUT":
			if _, err := fmt.Fprintf(p.stdin, "PUT\t%s\t%s\t%s\n", req.id,
				base64.StdEncoding.EncodeToString(op.key),
				base64.StdEncoding.EncodeToString(op.value)); err != nil {
				return err
			}
		case "DEL":
			if _, err := fmt.Fprintf(p.stdin, "DEL\t%s\t%s\n", req.id,
				base64.StdEncoding.EncodeToString(op.key)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported request operation %q", op.kind)
		}
		return nil
	}
	if len(req.ops) == 0 {
		return errors.New("empty request")
	}
	if _, err := fmt.Fprintf(p.stdin, "BATCH\t%s\t%d\n", req.id, len(req.ops)); err != nil {
		return err
	}
	for _, op := range req.ops {
		switch op.kind {
		case "PUT":
			if _, err := fmt.Fprintf(p.stdin, "PUT\t%s\t%s\n",
				base64.StdEncoding.EncodeToString(op.key),
				base64.StdEncoding.EncodeToString(op.value)); err != nil {
				return err
			}
		case "DEL":
			if _, err := fmt.Fprintf(p.stdin, "DEL\t%s\n",
				base64.StdEncoding.EncodeToString(op.key)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported batch operation %q", op.kind)
		}
	}
	return nil
}

func (p *javaProcess) check() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	line, err := p.commandLineLocked("CHECK")
	if err != nil {
		return 0, err
	}
	fields := strings.Split(line, "\t")
	if len(fields) != 3 || fields[0] != "CHECK" || fields[1] != "OK" {
		return 0, fmt.Errorf("unexpected CHECK response %q", line)
	}
	count, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (p *javaProcess) compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeCompaction.Store(true)
	defer p.activeCompaction.Store(false)
	line, err := p.commandLineLocked("COMPACT")
	if err != nil {
		return err
	}
	if line != "COMPACT OK" {
		return fmt.Errorf("unexpected COMPACT response %q", line)
	}
	return nil
}

func (p *javaProcess) dump() (map[string][]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for attempt := 0; attempt < 10; attempt++ {
		if p.cmd == nil {
			if err := p.startLocked(); err != nil {
				continue
			}
		}
		if _, err := fmt.Fprintln(p.stdin, "DUMP"); err != nil {
			p.stopLocked()
			continue
		}
		values := make(map[string][]byte)
		valid := true
		for {
			line, err := p.readLineLocked()
			if err != nil {
				valid = false
				break
			}
			fields := strings.Split(line, "\t")
			if len(fields) == 2 && fields[0] == "DUMP_DONE" {
				count, parseErr := strconv.Atoi(fields[1])
				if parseErr != nil || count != len(values) {
					return nil, fmt.Errorf("invalid DUMP count in %q", line)
				}
				return values, nil
			}
			if len(fields) != 3 || fields[0] != "ENTRY" {
				return nil, fmt.Errorf("unexpected DUMP response %q", line)
			}
			key, keyErr := base64.StdEncoding.DecodeString(fields[1])
			value, valueErr := base64.StdEncoding.DecodeString(fields[2])
			if keyErr != nil || valueErr != nil {
				return nil, fmt.Errorf("invalid Base64 DUMP entry")
			}
			values[string(key)] = value
		}
		if !valid {
			p.stopLocked()
		}
	}
	return nil, errors.New("Java process repeatedly terminated during DUMP")
}

// dumpFingerprint consumes DUMP one row at a time and never builds a second
// million-entry map in Go memory.
func (p *javaProcess) dumpFingerprint() (uint64, [32]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var zero [32]byte
	for attempt := 0; attempt < 10; attempt++ {
		if p.cmd == nil {
			if err := p.startLocked(); err != nil {
				continue
			}
		}
		if _, err := fmt.Fprintln(p.stdin, "DUMP"); err != nil {
			p.stopLocked()
			continue
		}
		var count uint64
		var fingerprint [32]byte
		for {
			line, err := p.readLineLocked()
			if err != nil {
				p.stopLocked()
				break
			}
			fields := strings.Split(line, "\t")
			if len(fields) == 2 && fields[0] == "DUMP_DONE" {
				responseCount, parseErr := strconv.ParseUint(fields[1], 10, 64)
				if parseErr != nil || responseCount != count {
					return 0, zero, fmt.Errorf("invalid DUMP count in %q", line)
				}
				return count, fingerprint, nil
			}
			if len(fields) != 3 || fields[0] != "ENTRY" {
				return 0, zero, fmt.Errorf("unexpected DUMP response %q", line)
			}
			key, keyErr := base64.StdEncoding.DecodeString(fields[1])
			value, valueErr := base64.StdEncoding.DecodeString(fields[2])
			if keyErr != nil || valueErr != nil {
				return 0, zero, errors.New("invalid Base64 DUMP entry")
			}
			xorFingerprint(&fingerprint, pairFingerprint(key, value))
			count++
		}
	}
	return 0, zero, errors.New("Java process repeatedly terminated during DUMP fingerprint")
}

func (p *javaProcess) commandLineLocked(command string) (string, error) {
	for attempt := 0; attempt < 10; attempt++ {
		if p.cmd == nil {
			if err := p.startLocked(); err != nil {
				continue
			}
		}
		if _, err := fmt.Fprintln(p.stdin, command); err != nil {
			p.stopLocked()
			continue
		}
		line, err := p.readLineLocked()
		if err != nil {
			p.stopLocked()
			continue
		}
		if strings.HasPrefix(line, "ERROR\t") {
			return "", parseRemoteError(line)
		}
		return line, nil
	}
	return "", errors.New("Java process repeatedly terminated")
}

func (p *javaProcess) readLineLocked() (string, error) {
	line, err := p.stdout.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

func parseRemoteError(line string) error {
	fields := strings.Split(line, "\t")
	if len(fields) != 3 || fields[0] != "ERROR" {
		return fmt.Errorf("Java returned error %q", line)
	}
	message, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		return fmt.Errorf("Java returned malformed error %q", line)
	}
	return fmt.Errorf("Java operation %s failed: %s", fields[1], string(message))
}

func (p *javaProcess) kill() killOutcome {
	p.procMu.Lock()
	defer p.procMu.Unlock()
	cmd := p.cmd
	if cmd == nil || cmd.Process == nil {
		return killNoProcess
	}
	err := cmd.Process.Kill()
	if err == nil {
		// A successful Kill call requests termination; Wait confirms that this
		// specific process actually exited before the harness counts the event.
		waitErr := cmd.Wait()
		p.cmd = nil
		if cmd.ProcessState != nil {
			_ = waitErr // Killed processes normally return *exec.ExitError from Wait.
			return killSucceeded
		}
		return killFailed
	}
	if errors.Is(err, os.ErrProcessDone) {
		return killAlreadyExited
	}
	return killFailed
}

func (p *javaProcess) stopLocked() {
	p.procMu.Lock()
	cmd := p.cmd
	p.cmd = nil
	p.procMu.Unlock()
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	if cmd != nil {
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
	p.stdout = nil
	p.stderr = nil
}

func (p *javaProcess) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.procMu.Lock()
	cmd := p.cmd
	p.cmd = nil
	p.procMu.Unlock()
	if cmd == nil {
		return
	}
	if p.stdin != nil {
		_, _ = fmt.Fprintln(p.stdin, "QUIT")
	}
	if p.stdout != nil {
		_, _ = p.readLineLocked() // BYE, or EOF if a chaos kill already occurred.
	}
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	if cmd.Process != nil && cmd.ProcessState == nil {
		_ = cmd.Wait()
	}
	p.stdout = nil
	p.stderr = nil
}
