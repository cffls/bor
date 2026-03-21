package blockstm

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

// OpcodeLevelExecutor extends the baseline BlockSTM executor with goroutine
// suspension. Instead of aborting on dependency conflicts, workers block until
// the dependency completes, then resume from the exact opcode.
//
// Architecture matches the baseline executor:
//   - Fixed worker pool (numProcs speculative + 1 guaranteed)
//   - Priority queue dispatch (lower tx indices first)
//   - Same-sender dependency tracking (prevents nonce retry storm)
//   - Validation and sequential settlement
//
// The key addition: when MVRead detects a FlagEstimate dependency, the worker
// goroutine blocks on a completion channel instead of panicking. A temporary
// replacement worker is spawned to prevent pool exhaustion. When the blocked
// worker resumes, the replacement exits after its current task.
type OpcodeLevelExecutor struct {
	tasks []ExecTask

	stats      map[int]ExecutionStat
	statsMutex sync.Mutex

	numSpeculativeProcs int

	// Channels for task dispatch — same pattern as baseline executor
	chTasks            chan ExecVersionView // guaranteed (next-in-order) tasks
	chSpeculativeTasks chan struct{}        // signal for speculative tasks
	specTaskQueue      SafeQueue            // priority queue of speculative tasks

	chSettle chan int
	settleWg sync.WaitGroup

	chResults   chan struct{}
	resultQueue SafeQueue

	workerWg sync.WaitGroup

	lastSettled int
	skipCheck   map[int]bool

	execTasks     taskStatusManager
	validateTasks taskStatusManager

	cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int
	diagExecSuccess, diagExecAbort                                        []int

	mvh      *MVHashMap
	lastTxIO *TxnInputOutput

	txIncarnations []int
	estimateDeps   map[int][]int
	preValidated   map[int]bool

	activeWorkers atomic.Int32

	// Opcode-level metrics (atomic — incremented from worker goroutines)
	cntSuspensions  atomic.Int64 // times a worker blocked in MVRead waiting for dependency
	cntReplacements atomic.Int64 // replacement workers spawned during suspensions

	// Pre-allocated replacement worker pool to avoid goroutine creation per suspension.
	chReplacementWake chan struct{} // signal channel to wake idle replacement workers

	// Conflict predictions: txIndex → list of predicted conflict keys.
	// Pre-populated as FlagEstimate in MVHashMap before workers start.
	predictions map[int][]Key

	begin   time.Time
	profile bool
}

var (
	opcodeLevelSettleWaitTimer     = metrics.NewRegisteredTimer("blockstm/opcode-level/settle/wait", nil)
	opcodeLevelSchedulerIdleTimer  = metrics.NewRegisteredTimer("blockstm/opcode-level/scheduler/idle", nil)
	opcodeLevelSchedulerStepTimer  = metrics.NewRegisteredTimer("blockstm/opcode-level/scheduler/step", nil)
	opcodeLevelValidationTimer     = metrics.NewRegisteredTimer("blockstm/opcode-level/validation", nil)
	opcodeLevelWorkerExecTimer     = metrics.NewRegisteredTimer("blockstm/opcode-level/worker/exec", nil)
	opcodeLevelWorkerFlushTimer    = metrics.NewRegisteredTimer("blockstm/opcode-level/worker/flush", nil)
	opcodeLevelPrepareTimer        = metrics.NewRegisteredTimer("blockstm/opcode-level/prepare", nil)
	opcodeLevelAbortCounter        = metrics.NewRegisteredCounter("blockstm/opcode-level/aborts", nil)
	opcodeLevelSuspensionCounter   = metrics.NewRegisteredCounter("blockstm/opcode-level/suspensions", nil)
	opcodeLevelReplacementCounter  = metrics.NewRegisteredCounter("blockstm/opcode-level/replacements", nil)
	opcodeLevelValidationFailCount = metrics.NewRegisteredCounter("blockstm/opcode-level/validation/fail", nil)
	opcodeLevelExecCount           = metrics.NewRegisteredCounter("blockstm/opcode-level/exec/count", nil)
	opcodeLevelTxCount             = metrics.NewRegisteredCounter("blockstm/opcode-level/tx/count", nil)

	// Parallelism metrics — measure actual concurrency achieved
	opcodeLevelActiveWorkersHist   = metrics.NewRegisteredHistogram("blockstm/opcode-level/active_workers", nil, metrics.NewUniformSample(10240))
	opcodeLevelDepChainDepthHist   = metrics.NewRegisteredHistogram("blockstm/opcode-level/dep_chain_depth", nil, metrics.NewUniformSample(10240))
	opcodeLevelBlockedTasksHist    = metrics.NewRegisteredHistogram("blockstm/opcode-level/blocked_tasks", nil, metrics.NewUniformSample(10240))
	opcodeLevelPendingTasksHist    = metrics.NewRegisteredHistogram("blockstm/opcode-level/pending_tasks", nil, metrics.NewUniformSample(10240))
	opcodeLevelInProgressTasksHist = metrics.NewRegisteredHistogram("blockstm/opcode-level/in_progress_tasks", nil, metrics.NewUniformSample(10240))
)

const numGoProcsOL = 1

func NewOpcodeLevelExecutor(tasks []ExecTask, profile bool, numProcs int) *OpcodeLevelExecutor {
	numTasks := len(tasks)

	resultQueue := NewSafePriorityQueue(numTasks)
	specTaskQueue := NewSafePriorityQueue(numTasks)

	return &OpcodeLevelExecutor{
		tasks:               tasks,
		numSpeculativeProcs: numProcs,
		stats:               make(map[int]ExecutionStat, numTasks),
		chTasks:             make(chan ExecVersionView, numTasks),
		chSpeculativeTasks:  make(chan struct{}, numTasks),
		specTaskQueue:       specTaskQueue,
		chSettle:            make(chan int, numTasks),
		chResults:           make(chan struct{}, numTasks),
		resultQueue:         resultQueue,
		lastSettled:         -1,
		skipCheck:           make(map[int]bool),
		execTasks:           makeStatusManager(numTasks),
		validateTasks:       makeStatusManager(0),
		diagExecSuccess:     make([]int, numTasks),
		diagExecAbort:       make([]int, numTasks),
		mvh:                 MakeMVHashMap(),
		lastTxIO:            MakeTxnInputOutput(numTasks),
		txIncarnations:      make([]int, numTasks),
		estimateDeps:        make(map[int][]int),
		preValidated:        make(map[int]bool),
		chReplacementWake:   make(chan struct{}, numTasks),
		begin:               time.Now(),
		profile:             profile,
	}
}

// Prepare sets up same-sender dependencies, launches workers, and bootstraps.
// Same-sender ordering is preserved to avoid nonce retry storms.
// Cross-sender conflicts are handled by goroutine suspension in MVRead.
func (pe *OpcodeLevelExecutor) Prepare() error {
	defer opcodeLevelPrepareTimer.UpdateSince(time.Now())
	// Same-sender dependency tracking — identical to baseline executor
	prevSenderTx := make(map[common.Address]int)

	for i, t := range pe.tasks {
		pe.skipCheck[i] = false
		pe.estimateDeps[i] = make([]int, 0)

		if len(t.Dependencies()) > 0 {
			clearPendingFlag := false

			for _, val := range t.Dependencies() {
				clearPendingFlag = true
				pe.execTasks.addDependencies(val, i)
			}

			if clearPendingFlag {
				pe.execTasks.clearPending(i)
			}
		} else {
			if tx, ok := prevSenderTx[t.Sender()]; ok {
				pe.execTasks.addDependencies(tx, i)
				pe.execTasks.clearPending(i)
			}

			prevSenderTx[t.Sender()] = i
		}
	}

	// Chain-style dispatch dependencies from predictions.
	// For each predicted conflict key, sort txs and chain each to its
	// immediate predecessor. This pipelines conflicting txs correctly:
	// tx N starts right after N-1 finishes, executes once without VFail.
	// Independent chains and non-predicted txs run fully in parallel.
	if pe.predictions != nil {
		keyTxs := make(map[Key][]int)

		for txIdx, keys := range pe.predictions {
			for _, k := range keys {
				keyTxs[k] = append(keyTxs[k], txIdx)
			}
		}

		for _, txIdxs := range keyTxs {
			if len(txIdxs) < 2 {
				continue
			}

			sort.Ints(txIdxs)

			for i := 1; i < len(txIdxs); i++ {
				if pe.execTasks.addDependencies(txIdxs[i-1], txIdxs[i]) {
					pe.execTasks.clearPending(txIdxs[i])
				}
			}
		}
	}

	// Compute dependency chain depth: the longest chain of same-sender
	// (or prediction-based) dependencies. This bounds the minimum serial
	// execution path — parallelism can't help beyond this.
	// Iteration order 0..N-1 is valid because addDependencies enforces
	// blocker < dependent, so depth[blocker] is always computed first.
	{
		numTx := len(pe.tasks)
		depth := make([]int, numTx)
		maxDepth := 0
		blockedCount := 0

		for i := 0; i < numTx; i++ {
			depth[i] = 1
			if pe.execTasks.isBlocked(i) {
				blockedCount++
			}

			// Propagate depth to all dependents of tx i
			if dependents, ok := pe.execTasks.dependency[i]; ok {
				for dep := range dependents {
					if depth[i]+1 > depth[dep] {
						depth[dep] = depth[i] + 1
					}
				}
			}

			if depth[i] > maxDepth {
				maxDepth = depth[i]
			}
		}

		opcodeLevelDepChainDepthHist.Update(int64(maxDepth))
		opcodeLevelBlockedTasksHist.Update(int64(blockedCount))
		opcodeLevelPendingTasksHist.Update(int64(len(pe.execTasks.pending)))
	}

	pe.mvh.SetOnWorkerSuspend(func() {
		pe.cntSuspensions.Add(1)
		pe.SpawnReplacementWorker()
	})

	workerCount := pe.numSpeculativeProcs + numGoProcsOL
	pe.workerWg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go pe.worker(i)
	}

	// Settlement goroutine
	pe.settleWg.Add(1)

	go func() {
		for t := range pe.chSettle {
			pe.tasks[t].Settle()
		}

		pe.settleWg.Done()
	}()

	// Bootstrap first execution
	tx := pe.execTasks.takeNextPending()
	if tx == -1 {
		return ParallelExecFailedError{"no executable transactions due to bad dependency"}
	}

	pe.cntExec++
	pe.chTasks <- ExecVersionView{ver: Version{tx, 0}, et: pe.tasks[tx], mvh: pe.mvh, sender: pe.tasks[tx].Sender()}

	return nil
}

func (pe *OpcodeLevelExecutor) worker(procNum int) {
	defer pe.workerWg.Done()

	execOne := func(task ExecVersionView) ExecResult {
		start := time.Duration(0)
		if pe.profile {
			start = time.Since(pe.begin)
		}

		pe.activeWorkers.Add(1)

		execStart := time.Now()
		res := task.Execute()
		opcodeLevelWorkerExecTimer.UpdateSince(execStart)

		pe.activeWorkers.Add(-1)

		txIdx := task.ver.TxnIndex

		if res.err == nil {
			flushStart := time.Now()
			pe.mvh.FlushMVWriteSet(res.txAllOut)
			opcodeLevelWorkerFlushTimer.UpdateSince(flushStart)
		}

		pe.mvh.NotifyCompletion(txIdx)

		pe.resultQueue.Push(txIdx, res)
		pe.chResults <- struct{}{}

		if pe.profile {
			end := time.Since(pe.begin)

			pe.statsMutex.Lock()
			pe.stats[txIdx] = ExecutionStat{
				TxIdx:       txIdx,
				Incarnation: res.ver.Incarnation,
				Start:       uint64(start),
				End:         uint64(end),
				Worker:      procNum,
			}
			pe.statsMutex.Unlock()
		}

		return res
	}

	doWork := func(task ExecVersionView) {
		execOne(task)
	}

	if procNum < pe.numSpeculativeProcs {
		for range pe.chSpeculativeTasks {
			taskVal := pe.specTaskQueue.(*SafePriorityQueue).TryPop()
			if taskVal == nil {
				continue
			}

			doWork(taskVal.(ExecVersionView))
		}
	} else {
		for task := range pe.chTasks {
			doWork(task)
		}
	}
}

func (pe *OpcodeLevelExecutor) Close(wait bool) {
	pe.mvh.Shutdown()

	close(pe.chTasks)
	close(pe.chSpeculativeTasks)
	close(pe.chSettle)

	if wait {
		// Drain chResults so workers unblocked by Shutdown() can send
		// their results without blocking (preventing workerWg deadlock).
		done := make(chan struct{})

		go func() {
			pe.workerWg.Wait()
			close(done)
		}()

		for {
			select {
			case <-done:
				settleWaitStart := time.Now()
				pe.settleWg.Wait()
				opcodeLevelSettleWaitTimer.UpdateSince(settleWaitStart)

				return
			case <-pe.chResults:
				// Drain — discard results from workers finishing after shutdown
			}
		}
	}
}

// Step processes a single execution result — same logic as baseline executor,
// with NotifyCompletion/ResetCompletion for goroutine suspension support.
// nolint:gocognit
func (pe *OpcodeLevelExecutor) Step(res *ExecResult) (result ParallelExecutionResult, err error) {
	tx := res.ver.TxnIndex

	if abortErr, ok := res.err.(ErrExecAbortError); ok && abortErr.OriginError != nil && pe.skipCheck[tx] {
		err = fmt.Errorf("could not apply tx %d [%v]: %w", tx, pe.tasks[tx].Hash(), abortErr.OriginError)
		pe.Close(true)

		return
	}

	// nolint:nestif
	if execErr, ok := res.err.(ErrExecAbortError); ok {
		addedDependencies := false

		if execErr.Dependency >= 0 {
			l := len(pe.estimateDeps[tx])
			for l > 0 && pe.estimateDeps[tx][l-1] > execErr.Dependency {
				pe.execTasks.removeDependency(pe.estimateDeps[tx][l-1])
				pe.estimateDeps[tx] = pe.estimateDeps[tx][:l-1]
				l--
			}

			addedDependencies = pe.execTasks.addDependencies(execErr.Dependency, tx)
		} else {
			estimate := 0

			if len(pe.estimateDeps[tx]) > 0 {
				estimate = pe.estimateDeps[tx][len(pe.estimateDeps[tx])-1]
			}

			addedDependencies = pe.execTasks.addDependencies(estimate, tx)

			newEstimate := estimate + (estimate+tx)/2
			if newEstimate >= tx {
				newEstimate = tx - 1
			}

			pe.estimateDeps[tx] = append(pe.estimateDeps[tx], newEstimate)
		}

		pe.execTasks.clearInProgress(tx)

		if !addedDependencies {
			pe.execTasks.pushPending(tx)
		}

		pe.txIncarnations[tx]++
		pe.mvh.ResetCompletion(tx)
		pe.diagExecAbort[tx]++
		pe.cntAbort++
	} else {
		pe.lastTxIO.recordRead(tx, res.txIn)

		if res.ver.Incarnation == 0 {
			pe.lastTxIO.recordWrite(tx, res.txOut)
			pe.lastTxIO.recordAllWrite(tx, res.txAllOut)
		} else {
			if res.txAllOut.hasNewWrite(pe.lastTxIO.AllWriteSet(tx)) {
				pe.validateTasks.pushPendingSet(pe.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := pe.lastTxIO.AllWriteSet(tx)

			cmpMap := make(map[Key]bool)

			for _, w := range res.txAllOut {
				cmpMap[w.Path] = true
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap[v.Path]; !ok {
					pe.mvh.Delete(v.Path, tx)
				}
			}

			pe.lastTxIO.recordWrite(tx, res.txOut)
			pe.lastTxIO.recordAllWrite(tx, res.txAllOut)
		}

		pe.validateTasks.pushPending(tx)
		pe.execTasks.markComplete(tx)

		pe.diagExecSuccess[tx]++
		pe.cntSuccess++

		pe.execTasks.removeDependency(tx)
	}

	// Validations
	maxComplete := pe.execTasks.maxAllComplete()

	toValidate := make([]int, 0, 2)

	for pe.validateTasks.minPending() <= maxComplete && pe.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, pe.validateTasks.takeNextPending())
	}

	validationStart := time.Now()

	for i := 0; i < len(toValidate); i++ {
		pe.cntTotalValidations++

		tx := toValidate[i]

		if pe.skipCheck[tx] || ValidateVersion(tx, pe.lastTxIO, pe.mvh) {
			pe.validateTasks.markComplete(tx)
		} else {
			pe.cntValidationFail++

			pe.diagExecAbort[tx]++

			for _, v := range pe.lastTxIO.AllWriteSet(tx) {
				pe.mvh.MarkEstimate(v.Path, tx)
			}

			pe.validateTasks.pushPendingSet(pe.execTasks.getRevalidationRange(tx + 1))
			pe.validateTasks.clearInProgress(tx)

			pe.execTasks.clearComplete(tx)
			pe.execTasks.pushPending(tx)

			pe.preValidated[tx] = false
			pe.txIncarnations[tx]++
			pe.mvh.ResetCompletion(tx)
		}
	}

	if len(toValidate) > 0 {
		opcodeLevelValidationTimer.UpdateSince(validationStart)
	}

	// Settlement
	maxValidated := pe.validateTasks.maxAllComplete()

	for pe.lastSettled < maxValidated {
		pe.lastSettled++
		if pe.execTasks.checkInProgress(pe.lastSettled) || pe.execTasks.checkPending(pe.lastSettled) || pe.execTasks.isBlocked(pe.lastSettled) {
			pe.lastSettled--
			break
		}

		pe.chSettle <- pe.lastSettled
	}

	if pe.validateTasks.countComplete() == len(pe.tasks) && pe.execTasks.countComplete() == len(pe.tasks) {
		log.Debug("blockstm opcode-level exec summary", "execs", pe.cntExec, "success", pe.cntSuccess, "aborts", pe.cntAbort, "validations", pe.cntTotalValidations, "failures", pe.cntValidationFail, "suspensions", pe.cntSuspensions.Load(), "replacements", pe.cntReplacements.Load(), "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(pe.tasks))/float64(pe.cntExec)*100))

		pe.Close(true)

		var allDeps map[int]map[int]bool

		var deps DAG

		if pe.profile {
			allDeps = GetDep(*pe.lastTxIO)
			deps = BuildDAG(*pe.lastTxIO)
		}

		return ParallelExecutionResult{
			TxIO: pe.lastTxIO, Stats: &pe.stats, Deps: &deps, AllDeps: allDeps,
			Aborts: pe.cntAbort, Suspensions: pe.cntSuspensions.Load(),
			Executions: pe.cntExec, ValidationFails: pe.cntValidationFail,
			Replacements: pe.cntReplacements.Load(),
		}, err
	}

	// Dispatch next guaranteed task
	if pe.execTasks.minPending() != -1 && pe.execTasks.minPending() == maxValidated+1 {
		nextTx := pe.execTasks.takeNextPending()
		if nextTx != -1 {
			pe.cntExec++
			pe.skipCheck[nextTx] = true
			pe.chTasks <- ExecVersionView{ver: Version{nextTx, pe.txIncarnations[nextTx]}, et: pe.tasks[nextTx], mvh: pe.mvh, sender: pe.tasks[nextTx].Sender()}
		}
	}

	// Dispatch speculative tasks (priority queue — lowest index first)
	for pe.execTasks.minPending() != -1 {
		nextTx := pe.execTasks.takeNextPending()
		if nextTx != -1 {
			pe.cntExec++
			task := ExecVersionView{ver: Version{nextTx, pe.txIncarnations[nextTx]}, et: pe.tasks[nextTx], mvh: pe.mvh, sender: pe.tasks[nextTx].Sender()}
			pe.specTaskQueue.Push(nextTx, task)
			pe.chSpeculativeTasks <- struct{}{}
		}
	}

	return
}

// SpawnReplacementWorker is called when a worker suspends in MVRead.
// Non-blocking: picks up a queued task if available, otherwise exits.
func (pe *OpcodeLevelExecutor) SpawnReplacementWorker() {
	pe.cntReplacements.Add(1)
	pe.workerWg.Add(1)

	go func() {
		defer pe.workerWg.Done()

		select {
		case <-pe.chSpeculativeTasks:
			taskVal := pe.specTaskQueue.(*SafePriorityQueue).TryPop()
			if taskVal == nil {
				return
			}

			pe.doReplacementWork(taskVal.(ExecVersionView))
		default:
		}
	}()
}

// doReplacementWork executes a single task from a replacement worker.
func (pe *OpcodeLevelExecutor) doReplacementWork(task ExecVersionView) {
	txIdx := task.ver.TxnIndex

	res := task.Execute()

	if res.err == nil {
		pe.mvh.FlushMVWriteSet(res.txAllOut)
	}

	pe.mvh.NotifyCompletion(txIdx)
	pe.resultQueue.Push(txIdx, res)
	pe.chResults <- struct{}{}
}

type PropertyCheckOL func(*OpcodeLevelExecutor) error

func executeOpcodeLevelWithCheck(tasks []ExecTask, profile bool, numProcs int, predictions map[int][]Key, interruptCtx context.Context) (result ParallelExecutionResult, err error) {
	if len(tasks) == 0 {
		return ParallelExecutionResult{TxIO: MakeTxnInputOutput(len(tasks))}, nil
	}

	pe := NewOpcodeLevelExecutor(tasks, profile, numProcs)
	pe.predictions = predictions
	err = pe.Prepare()

	if err != nil {
		pe.Close(true)
		return
	}

	idleStart := time.Now()

	for range pe.chResults {
		opcodeLevelSchedulerIdleTimer.UpdateSince(idleStart)

		// Sample concurrency: how many workers are actively executing right now
		opcodeLevelActiveWorkersHist.Update(int64(pe.activeWorkers.Load()))
		opcodeLevelInProgressTasksHist.Update(int64(len(pe.execTasks.inProgress)))

		if interruptCtx != nil && interruptCtx.Err() != nil {
			pe.Close(true)
			return result, interruptCtx.Err()
		}

		res := pe.resultQueue.Pop().(ExecResult)

		stepStart := time.Now()
		result, err = pe.Step(&res)
		opcodeLevelSchedulerStepTimer.UpdateSince(stepStart)

		if err != nil {
			return result, err
		}

		if result.TxIO != nil || err != nil {
			opcodeLevelAbortCounter.Inc(int64(pe.cntAbort))
			opcodeLevelSuspensionCounter.Inc(pe.cntSuspensions.Load())
			opcodeLevelReplacementCounter.Inc(pe.cntReplacements.Load())
			opcodeLevelValidationFailCount.Inc(int64(pe.cntValidationFail))
			opcodeLevelExecCount.Inc(int64(pe.cntExec))
			opcodeLevelTxCount.Inc(int64(len(tasks)))
			return result, err
		}

		idleStart = time.Now()
	}

	return
}

func ExecuteParallelOpcodeLevel(tasks []ExecTask, profile bool, numProcs int, predictions map[int][]Key, interruptCtx context.Context) (result ParallelExecutionResult, err error) {
	return executeOpcodeLevelWithCheck(tasks, profile, numProcs, predictions, interruptCtx)
}
