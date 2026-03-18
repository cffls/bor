package blockstm

import (
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// lateConflictTask simulates a transaction that does significant independent
// work (nonIO ops) BEFORE reading a shared key that conflicts with an earlier tx.
// In opcode-level mode, it blocks via WaitForTx at the conflict point instead
// of aborting. In baseline mode, it aborts and re-executes everything.
type lateConflictTask struct {
	txIdx        int
	sender       common.Address
	nonce        int
	dependencies []int
	readMap      map[Key]ReadDescriptor
	writeMap     map[Key]WriteDescriptor

	// Task structure: [nonce read/write] [independent work] [shared key read/write]
	independentWork time.Duration // work before the conflict
	sharedKey       Key           // the conflicting key (shared across senders)
	privateKey      Key           // unique key per sender (nonce)
	opDuration      time.Duration // duration per IO op

	// If true, use WaitForTx to block on dependency (opcode-level simulation).
	// If false, return ErrExecAbortError immediately (baseline simulation).
	useWaitForTx bool
}

func (t *lateConflictTask) Execute(mvh *MVHashMap, incarnation int) (retErr error) {
	if t.useWaitForTx {
		defer func() {
			if r := recover(); r != nil {
				retErr = ErrExecAbortError{Dependency: -1}
			}
		}()
	}

	version := Version{TxnIndex: t.txIdx, Incarnation: incarnation}
	t.readMap = make(map[Key]ReadDescriptor)
	t.writeMap = make(map[Key]WriteDescriptor)

	// Step 1: Read nonce (private key — no cross-sender conflict)
	nonceResult := mvh.Read(t.privateKey, t.txIdx)
	if nonceResult.Status() == MVReadResultDependency {
		if t.useWaitForTx {
			// Block until dependency completes (opcode-level)
			mvh.OnWorkerSuspend()
			ch := mvh.WaitForTx(nonceResult.DepIdx())

			select {
			case <-ch:
				// Retry the nonce read
				nonceResult = mvh.Read(t.privateKey, t.txIdx)
			case <-mvh.ShutdownCh():
				panic("shutdown")
			}
		} else {
			return ErrExecAbortError{nonceResult.DepIdx(), nil}
		}
	}

	if nonceResult.Status() == MVReadResultDone {
		t.readMap[t.privateKey] = ReadDescriptor{t.privateKey, ReadKindMap, Version{nonceResult.DepIdx(), nonceResult.Incarnation()}}
	} else {
		t.readMap[t.privateKey] = ReadDescriptor{t.privateKey, ReadKindStorage, Version{-1, -1}}
	}

	// Step 2: Write nonce
	t.writeMap[t.privateKey] = WriteDescriptor{t.privateKey, version, t.nonce + 1}

	// Step 3: Independent work — this is the work SAVED by opcode-level on retry
	sleep(t.independentWork)

	// Step 4: Read shared key (LATE conflict point)
	sharedResult := mvh.Read(t.sharedKey, t.txIdx)
	if sharedResult.Status() == MVReadResultDependency {
		if t.useWaitForTx {
			// Block until dependency completes (opcode-level)
			mvh.OnWorkerSuspend()
			ch := mvh.WaitForTx(sharedResult.DepIdx())

			select {
			case <-ch:
				sharedResult = mvh.Read(t.sharedKey, t.txIdx)
			case <-mvh.ShutdownCh():
				panic("shutdown")
			}
		} else {
			return ErrExecAbortError{sharedResult.DepIdx(), nil}
		}
	}

	if sharedResult.Status() == MVReadResultDone {
		t.readMap[t.sharedKey] = ReadDescriptor{t.sharedKey, ReadKindMap, Version{sharedResult.DepIdx(), sharedResult.Incarnation()}}
	} else {
		t.readMap[t.sharedKey] = ReadDescriptor{t.sharedKey, ReadKindStorage, Version{-1, -1}}
	}

	sleep(t.opDuration)

	// Step 5: Write shared key
	t.writeMap[t.sharedKey] = WriteDescriptor{t.sharedKey, version, t.txIdx}

	return nil
}

func (t *lateConflictTask) MVWriteList() []WriteDescriptor  { return t.MVFullWriteList() }
func (t *lateConflictTask) MVFullWriteList() []WriteDescriptor {
	writes := make([]WriteDescriptor, 0, len(t.writeMap))
	for _, v := range t.writeMap {
		writes = append(writes, v)
	}
	return writes
}
func (t *lateConflictTask) MVReadList() []ReadDescriptor {
	reads := make([]ReadDescriptor, 0, len(t.readMap))
	for _, v := range t.readMap {
		reads = append(reads, v)
	}
	return reads
}
func (t *lateConflictTask) Settle()                {}
func (t *lateConflictTask) Sender() common.Address { return t.sender }
func (t *lateConflictTask) Hash() common.Hash {
	return common.BytesToHash([]byte(fmt.Sprintf("%d", t.txIdx)))
}
func (t *lateConflictTask) Dependencies() []int { return t.dependencies }

// makeLateConflictTasks creates numTx transactions from distinct senders that
// all read/write a shared key AFTER doing independentWork of computation.
// This is the ideal scenario for opcode-level BlockSTM: the baseline must
// re-execute all the independent work on every retry, while opcode-level
// blocks at the conflict and resumes without re-doing the work.
func makeLateConflictTasks(numTx int, independentWork time.Duration, useWaitForTx bool) []ExecTask {
	sharedKey := NewSubpathKey(common.BigToAddress(big.NewInt(999)), 1)
	opDuration := 5 * time.Microsecond

	tasks := make([]ExecTask, numTx)
	for i := 0; i < numTx; i++ {
		sender := common.BigToAddress(big.NewInt(int64(i))) // unique sender per tx
		tasks[i] = &lateConflictTask{
			txIdx:           i,
			sender:          sender,
			nonce:           0,
			dependencies:    []int{},
			readMap:         make(map[Key]ReadDescriptor),
			writeMap:        make(map[Key]WriteDescriptor),
			independentWork: independentWork,
			sharedKey:       sharedKey,
			privateKey:      NewSubpathKey(sender, 2), // nonce key
			opDuration:      opDuration,
			useWaitForTx:    useWaitForTx,
		}
	}

	return tasks
}

// TestLateConflictComparison compares baseline vs opcode-level execution on
// transactions with late cross-sender conflicts.
//
// Expected: opcode-level should be faster because it blocks at the conflict
// point instead of re-executing all the independent work before it.
func TestLateConflictComparison(t *testing.T) {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, false)))

	for _, numTx := range []int{10, 20, 50} {
		for _, workDuration := range []time.Duration{
			100 * time.Microsecond,
			500 * time.Microsecond,
			1 * time.Millisecond,
		} {
			t.Run(fmt.Sprintf("txs=%d/work=%v", numTx, workDuration), func(t *testing.T) {
				// Run baseline (abort + re-execute)
				baselineTasks := makeLateConflictTasks(numTx, workDuration, false)
				baselineStart := time.Now()
				baselineResult, err := ExecuteParallel(baselineTasks, false, false, numProcs, nil)
				baselineDuration := time.Since(baselineStart)
				if err != nil {
					t.Fatalf("baseline error: %v", err)
				}

				// Run opcode-level (block + resume)
				olTasks := makeLateConflictTasks(numTx, workDuration, true)
				olStart := time.Now()
				olResult, err := ExecuteParallelOpcodeLevel(olTasks, false, numProcs, nil, nil)
				olDuration := time.Since(olStart)
				if err != nil {
					t.Fatalf("opcode-level error: %v", err)
				}

				_ = baselineResult
				_ = olResult

				speedup := float64(baselineDuration) / float64(olDuration)
				saved := baselineDuration - olDuration

				marker := "  "
				if olDuration < baselineDuration {
					marker = "✓ "
				}

				fmt.Printf("%stxs=%-3d work=%-12v  baseline=%-12v  opcode-level=%-12v  speedup=%.2fx  saved=%v\n",
					marker, numTx, workDuration, baselineDuration, olDuration, speedup, saved)
			})
		}
	}
}

// BenchmarkLateConflict provides stable benchmark numbers for both executors.
func BenchmarkLateConflict(b *testing.B) {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelError, false)))

	numTx := 30
	workDuration := 500 * time.Microsecond

	b.Run("Baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tasks := makeLateConflictTasks(numTx, workDuration, false)
			_, err := ExecuteParallel(tasks, false, false, numProcs, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("OpcodeLevel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tasks := makeLateConflictTasks(numTx, workDuration, true)
			_, err := ExecuteParallelOpcodeLevel(tasks, false, numProcs, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
