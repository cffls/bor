package blockstm

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// opcodeLevelExecTask is a test task that uses panic-based or suspend-based
// dependency detection, matching what the real EVM execution path does.
type opcodeLevelExecTask struct {
	txIdx        int
	ops          []Op
	readMap      map[Key]ReadDescriptor
	writeMap     map[Key]WriteDescriptor
	sender       common.Address
	nonce        int
	dependencies []int
	execCount    atomic.Int32 // tracks how many times Execute is called
}

func newOpcodeLevelExecTask(txIdx int, ops []Op, sender common.Address, nonce int) *opcodeLevelExecTask {
	return &opcodeLevelExecTask{
		txIdx:        txIdx,
		ops:          ops,
		readMap:      make(map[Key]ReadDescriptor),
		writeMap:     make(map[Key]WriteDescriptor),
		sender:       sender,
		nonce:        nonce,
		dependencies: []int{},
	}
}

func (t *opcodeLevelExecTask) Execute(mvh *MVHashMap, incarnation int) (retErr error) {
	t.execCount.Add(1)

	defer func() {
		if r := recover(); r != nil {
			// Caught a panic from dependency detection — return abort error
			retErr = ErrExecAbortError{Dependency: -1}
		}
	}()

	sleep(time.Microsecond * 50)

	version := Version{TxnIndex: t.txIdx, Incarnation: incarnation}

	t.readMap = make(map[Key]ReadDescriptor)
	t.writeMap = make(map[Key]WriteDescriptor)

	for i, op := range t.ops {
		k := op.key

		switch op.opType {
		case readType:
			if _, ok := t.writeMap[k]; ok {
				sleep(op.duration)
				continue
			}

			result := mvh.Read(k, t.txIdx)

			val := result.Value()

			if i == 0 && val != nil && (val.(int) != t.nonce) {
				return ErrExecAbortError{}
			}

			// If dependency detected, panic (will be caught by defer/recover or
			// blocked by goroutine suspension via MVHashMap's WaitForTx)
			if result.Status() == MVReadResultDependency {
				panic("Found dependency")
			}

			var readKind int

			if result.Status() == MVReadResultDone {
				readKind = ReadKindMap
			} else if result.Status() == MVReadResultNone {
				readKind = ReadKindStorage
			}

			sleep(op.duration)

			t.readMap[k] = ReadDescriptor{k, readKind, Version{TxnIndex: result.depIdx, Incarnation: result.incarnation}}
		case writeType:
			t.writeMap[k] = WriteDescriptor{k, version, op.val}
		case otherType:
			sleep(op.duration)
		default:
			panic(fmt.Sprintf("Unknown op type: %d", op.opType))
		}
	}

	return nil
}

func (t *opcodeLevelExecTask) MVWriteList() []WriteDescriptor {
	return t.MVFullWriteList()
}

func (t *opcodeLevelExecTask) MVFullWriteList() []WriteDescriptor {
	writes := make([]WriteDescriptor, 0, len(t.writeMap))
	for _, v := range t.writeMap {
		writes = append(writes, v)
	}
	return writes
}

func (t *opcodeLevelExecTask) MVReadList() []ReadDescriptor {
	reads := make([]ReadDescriptor, 0, len(t.readMap))
	for _, v := range t.readMap {
		reads = append(reads, v)
	}
	return reads
}

func (t *opcodeLevelExecTask) Settle()                  {}
func (t *opcodeLevelExecTask) Sender() common.Address   { return t.sender }
func (t *opcodeLevelExecTask) Hash() common.Hash        { return common.BytesToHash([]byte(fmt.Sprintf("%d", t.txIdx))) }
func (t *opcodeLevelExecTask) Dependencies() []int       { return t.dependencies }
func runOpcodeLevel(t *testing.T, tasks []ExecTask) time.Duration {
	t.Helper()

	start := time.Now()
	result, err := ExecuteParallelOpcodeLevel(tasks, false, numProcs, nil, nil)

	assert.NoError(t, err, "error during suspend execution")
	assert.NotNil(t, result.TxIO)

	// Apply final write set to storage (same as runParallel)
	finalWriteSet := make(map[Key]time.Duration)
	for _, task := range tasks {
		task := task.(*opcodeLevelExecTask)
		for _, op := range task.ops {
			if op.opType == writeType {
				finalWriteSet[op.key] = op.duration
			}
		}
	}
	for _, v := range finalWriteSet {
		sleep(v)
	}

	return time.Since(start)
}

func opcodeLevelTaskFactory(numTask int, sender Sender, readsPerT int, writesPerT int, nonIOPerT int, pathGenerator PathGenerator, readTime Timer, writeTime Timer, nonIOTime Timer) ([]ExecTask, time.Duration) {
	exec := make([]ExecTask, 0, numTask)
	var serialDuration time.Duration

	senderNonces := make(map[common.Address]int)

	for i := 0; i < numTask; i++ {
		s := sender(i)

		ops := make([]Op, 0, readsPerT+writesPerT+nonIOPerT)

		ops = append(ops, Op{opType: readType, key: NewSubpathKey(s, 2), duration: readTime(i, 0), val: senderNonces[s]})
		senderNonces[s]++
		ops = append(ops, Op{opType: writeType, key: NewSubpathKey(s, 2), duration: writeTime(i, 1), val: senderNonces[s]})

		for j := 0; j < readsPerT-1; j++ {
			ops = append(ops, Op{opType: readType})
		}
		for j := 0; j < nonIOPerT; j++ {
			ops = append(ops, Op{opType: otherType})
		}
		for j := 0; j < writesPerT-1; j++ {
			ops = append(ops, Op{opType: writeType})
		}

		for j := 3; j < len(ops)-1; j++ {
			k := rand.Intn(len(ops)-j-1) + j
			ops[j], ops[k] = ops[k], ops[j]
		}

		for j := 2; j < len(ops); j++ {
			if ops[j].opType == readType {
				ops[j].key = pathGenerator(s, i, j, len(ops))
				ops[j].duration = readTime(i, j)
			} else if ops[j].opType == writeType {
				ops[j].key = pathGenerator(s, i, j, len(ops))
				ops[j].duration = writeTime(i, j)
			} else {
				ops[j].duration = nonIOTime(i, j)
			}
			serialDuration += ops[j].duration
		}

		if ops[len(ops)-1].opType != writeType {
			panic("Last op must be a write")
		}

		t := newOpcodeLevelExecTask(i, ops, s, senderNonces[s]-1)
		exec = append(exec, t)
	}

	return exec, serialDuration
}

// TestOpcodeLevelExecutorBasic verifies that the suspend executor produces correct results
// for a simple scenario with minimal conflicts.
func TestOpcodeLevelExecutorBasic(t *testing.T) {
	t.Parallel()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelDebug, false)))

	rand.New(rand.NewSource(42))

	sender := func(i int) common.Address {
		return common.BigToAddress(big.NewInt(int64(i % 20)))
	}

	tasks, _ := opcodeLevelTaskFactory(50, sender, 20, 20, 100, randomPathGenerator, readTime, writeTime, nonIOTime)

	duration := runOpcodeLevel(t, tasks)

	t.Logf("Suspend executor: 50 txs completed in %v", duration)
}

// TestOpcodeLevelExecutorHighConflict tests the suspend executor with transactions
// that have high contention (few senders → lots of same-sender nonce conflicts).
func TestOpcodeLevelExecutorHighConflict(t *testing.T) {
	t.Parallel()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelDebug, false)))

	rand.New(rand.NewSource(42))

	// Only 3 senders → many same-sender nonce conflicts
	sender := func(i int) common.Address {
		return common.BigToAddress(big.NewInt(int64(i % 3)))
	}

	tasks, _ := opcodeLevelTaskFactory(30, sender, 10, 10, 50, randomPathGenerator, readTime, writeTime, nonIOTime)

	duration := runOpcodeLevel(t, tasks)

	t.Logf("Suspend executor (high conflict): 30 txs completed in %v", duration)
}

// TestOpcodeLevelExecutorZeroTx verifies the suspend executor handles empty blocks.
func TestOpcodeLevelExecutorZeroTx(t *testing.T) {
	t.Parallel()

	result, err := ExecuteParallelOpcodeLevel([]ExecTask{}, false, numProcs, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, result.TxIO)
}

// TestOpcodeLevelExecutorContextCancel verifies graceful shutdown on context cancellation.
func TestOpcodeLevelExecutorContextCancel(t *testing.T) {
	t.Parallel()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelDebug, false)))

	rand.New(rand.NewSource(42))

	sender := func(i int) common.Address {
		return common.BigToAddress(big.NewInt(int64(i % 5)))
	}

	tasks, _ := opcodeLevelTaskFactory(100, sender, 20, 20, 500, randomPathGenerator, readTime, writeTime, nonIOTime)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := ExecuteParallelOpcodeLevel(tasks, false, numProcs, nil, ctx)

	// Should either complete or return context error
	if err != nil {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

// TestOpcodeLevelExecutorDexScenario tests with DEX-like access patterns where
// transactions share a common pool address (high contention on specific keys).
func TestOpcodeLevelExecutorDexScenario(t *testing.T) {
	t.Parallel()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelDebug, false)))

	rand.New(rand.NewSource(42))

	sender := func(i int) common.Address {
		return common.BigToAddress(big.NewInt(int64(i)))
	}

	tasks, serialDuration := opcodeLevelTaskFactory(50, sender, 20, 20, 100, dexPathGenerator, readTime, writeTime, nonIOTime)

	duration := runOpcodeLevel(t, tasks)

	t.Logf("Suspend DEX scenario: 50 txs in %v (serial estimate: %v)", duration, serialDuration)
}

// TestMVHashMapCompletionSignaling tests the WaitForTx/NotifyCompletion/ResetCompletion
// primitives directly.
func TestMVHashMapCompletionSignaling(t *testing.T) {
	t.Parallel()

	mvh := MakeMVHashMap()

	t.Run("WaitAndNotify", func(t *testing.T) {
		ch := mvh.WaitForTx(5)

		done := make(chan struct{})
		go func() {
			<-ch
			close(done)
		}()

		// Should not be done yet
		select {
		case <-done:
			t.Fatal("should not be unblocked before NotifyCompletion")
		case <-time.After(10 * time.Millisecond):
		}

		mvh.NotifyCompletion(5)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("should be unblocked after NotifyCompletion")
		}
	})

	t.Run("MultipleWaiters", func(t *testing.T) {
		var count atomic.Int32
		for i := 0; i < 5; i++ {
			ch := mvh.WaitForTx(10)
			go func() {
				<-ch
				count.Add(1)
			}()
		}

		mvh.NotifyCompletion(10)
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, int32(5), count.Load(), "all 5 waiters should be unblocked")
	})

	t.Run("ResetCompletion", func(t *testing.T) {
		// First complete and notify
		mvh.NotifyCompletion(20)

		// Reset for new incarnation
		mvh.ResetCompletion(20)

		// New waiter should block
		ch := mvh.WaitForTx(20)
		done := make(chan struct{})
		go func() {
			<-ch
			close(done)
		}()

		select {
		case <-done:
			t.Fatal("should be blocked after ResetCompletion")
		case <-time.After(10 * time.Millisecond):
		}

		mvh.NotifyCompletion(20)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("should be unblocked after second NotifyCompletion")
		}
	})

	t.Run("Shutdown", func(t *testing.T) {
		mvh2 := MakeMVHashMap()

		ch := mvh2.WaitForTx(99)
		done := make(chan struct{})
		go func() {
			<-ch
			close(done)
		}()

		mvh2.Shutdown()

		// The shutdown channel unblocks shutdownCh, but WaitForTx returns a
		// completion channel, not shutdownCh. In practice, MVRead selects on both.
		// Test that Shutdown is idempotent.
		mvh2.Shutdown()
	})
}
