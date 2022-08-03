package blockstm

import (
	"fmt"
)

type ExecResult struct {
	err      error
	ver      Version
	txIn     TxnInput
	txOut    TxnOutput
	txAllOut TxnOutput
}

type ExecTask interface {
	Execute(mvh *MVHashMap, incarnation int) error
	MVReadList() []ReadDescriptor
	MVWriteList() []WriteDescriptor
	MVFullWriteList() []WriteDescriptor
}

type ExecVersionView struct {
	ver Version
	et  ExecTask
	mvh *MVHashMap
}

func (ev *ExecVersionView) Execute() (er ExecResult) {
	er.ver = ev.ver
	if er.err = ev.et.Execute(ev.mvh, ev.ver.Incarnation); er.err != nil {
		return
	}

	er.txIn = ev.et.MVReadList()
	er.txOut = ev.et.MVWriteList()
	er.txAllOut = ev.et.MVFullWriteList()

	return
}

type ErrExecAbort struct {
	DependencyIndex int
}

func (e ErrExecAbort) Error() string {
	if e.DependencyIndex >= 0 {
		return fmt.Sprintf("Execution aborted due to dependency %d", e.DependencyIndex)
	} else {
		return "Execution aborted"
	}
}

const numGoProcs = 6

// nolint: gocognit
func ExecuteParallel(tasks []ExecTask) (lastTxIO *TxnInputOutput, err error) {
	if len(tasks) == 0 {
		return MakeTxnInputOutput(len(tasks)), nil
	}

	chTasks := make(chan ExecVersionView, len(tasks))
	chResults := make(chan ExecResult, len(tasks))
	chDone := make(chan bool)

	var cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	mvh := MakeMVHashMap()

	for i := 0; i < numGoProcs; i++ {
		go func(procNum int, t chan ExecVersionView) {
		Loop:
			for {
				select {
				case task := <-t:
					{
						res := task.Execute()
						if res.err == nil {
							mvh.FlushMVWriteSet(res.txAllOut)
						}
						chResults <- res
					}
				case <-chDone:
					break Loop
				}
			}
		}(i, chTasks)
	}

	execTasks := makeStatusManager(len(tasks))
	validateTasks := makeStatusManager(0)

	// bootstrap execution
	for x := 0; x < numGoProcs; x++ {
		tx := execTasks.takeNextPending()
		if tx != -1 {
			cntExec++

			chTasks <- ExecVersionView{ver: Version{tx, 0}, et: tasks[tx], mvh: mvh}
		}
	}

	lastTxIO = MakeTxnInputOutput(len(tasks))
	txIncarnations := make([]int, len(tasks))

	diagExecSuccess := make([]int, len(tasks))
	diagExecAbort := make([]int, len(tasks))

	for {
		res := <-chResults
		if res.err == nil {
			lastTxIO.recordRead(res.ver.TxnIndex, res.txIn)
			if res.ver.Incarnation == 0 {
				lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				lastTxIO.recordAllWrite(res.ver.TxnIndex, res.txAllOut)
			} else {
				if res.txAllOut.hasNewWrite(lastTxIO.AllWriteSet(res.ver.TxnIndex)) {
					validateTasks.pushPendingSet(execTasks.getRevalidationRange(res.ver.TxnIndex + 1))
				}

				prevWrite := lastTxIO.AllWriteSet(res.ver.TxnIndex)

				// Remove entries that were previously written but are no longer written

				cmpMap := make(map[Key]bool)

				for _, w := range res.txAllOut {
					cmpMap[w.Path] = true
				}

				for _, v := range prevWrite {
					if _, ok := cmpMap[v.Path]; !ok {
						mvh.Delete(v.Path, res.ver.TxnIndex)
					}
				}

				lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				lastTxIO.recordAllWrite(res.ver.TxnIndex, res.txAllOut)
			}
			validateTasks.pushPending(res.ver.TxnIndex)
			execTasks.markComplete(res.ver.TxnIndex)
			diagExecSuccess[res.ver.TxnIndex]++
			cntSuccess++
			execTasks.removeDependency(res.ver.TxnIndex)
		} else if execErr, ok := res.err.(ErrExecAbort); ok {
			if execErr.DependencyIndex >= 0 {
				execTasks.clearInProgress(res.ver.TxnIndex)
				if !execTasks.addDependency(execErr.DependencyIndex, res.ver.TxnIndex) {
					execTasks.pushPending(res.ver.TxnIndex)
				}
			} else {
				execTasks.revertInProgress(res.ver.TxnIndex)
			}
			txIncarnations[res.ver.TxnIndex]++
			diagExecAbort[res.ver.TxnIndex]++
			cntAbort++
		} else {
			err = res.err
			break
		}

		// if we got more work, queue one up...
		nextTx := execTasks.takeNextPending()
		if nextTx != -1 {
			cntExec++
			chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh}
		}

		// do validations ...
		maxComplete := execTasks.maxAllComplete()

		const validationIncrement = 32

		cntValidate := validateTasks.countPending()
		// if we're currently done with all execution tasks then let's validate everything; otherwise do one increment ...
		if execTasks.countComplete() != len(tasks) && cntValidate > validationIncrement {
			cntValidate = validationIncrement
		}

		var toValidate []int

		for validateTasks.minPending() != -1 {
			if validateTasks.minPending() <= maxComplete && validateTasks.minPending() >= 0 {
				toValidate = append(toValidate, validateTasks.takeNextPending())
			} else {
				break
			}
		}

		for i := 0; i < len(toValidate); i++ {
			cntTotalValidations++

			tx := toValidate[i]

			if ValidateVersion(tx, lastTxIO, mvh) {
				validateTasks.markComplete(tx)
			} else {
				cntValidationFail++
				diagExecAbort[tx]++
				for _, v := range lastTxIO.AllWriteSet(tx) {
					mvh.MarkEstimate(v.Path, tx)
				}
				// 'create validation tasks for all transactions > tx ...'
				validateTasks.pushPendingSet(execTasks.getRevalidationRange(tx + 1))
				validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes
				if execTasks.checkPending(tx) {
					// println() // have to think about this ...
				} else {
					execTasks.pushPending(tx)
					execTasks.clearComplete(tx)
					txIncarnations[tx]++
				}
			}
		}

		// if we didn't queue work previously, do check again so we keep making progress ...
		for execTasks.minPending() != -1 {
			nextTx = execTasks.takeNextPending()
			if nextTx != -1 {
				cntExec++

				chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh}
			}
		}

		if validateTasks.countComplete() == len(tasks) && execTasks.countComplete() == len(tasks) {
			break
		}
	}

	for i := 0; i < numGoProcs; i++ {
		chDone <- true
	}
	close(chTasks)
	close(chResults)

	return
}
