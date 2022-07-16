package blockstm

import (
	"fmt"
)

type ExecResult struct {
	err   error
	ver   Version
	txIn  TxnInput
	txOut TxnOutput
}

type ExecTask interface {
	Execute(mvh *MVHashMap, incarnation int) error
	MVReadList() []ReadDescriptor
	MVWriteList() []WriteDescriptor
}

type ExecVersionView struct {
	ver Version
	et  ExecTask
	mvh *MVHashMap
}

func (ev *ExecVersionView) Execute() (er ExecResult) {
	er.ver = ev.ver
	if er.err = ev.et.Execute(ev.mvh, ev.ver.Incarnation); er.err != nil {
		println(fmt.Sprintf("executed task - failed %v.%v, err %v", ev.ver.TxnIndex, ev.ver.Incarnation, er.err))
		return
	}
	er.txIn = ev.et.MVReadList()
	er.txOut = ev.et.MVWriteList()
	println(fmt.Sprintf("executed task %v.%v, in %v, out %v", ev.ver.TxnIndex, ev.ver.Incarnation,
		len(er.txIn), len(er.txOut)))
	return
}

var ErrExecAbort = fmt.Errorf("execution aborted with dependency")

const numGoProcs = 10

func ExecuteParallel(tasks []ExecTask) (lastTxIO *TxnInputOutput, err error) {

	chTasks := make(chan ExecVersionView, len(tasks))
	chResults := make(chan ExecResult, len(tasks))
	chDone := make(chan bool)

	var cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	for i := 0; i < numGoProcs; i++ {
		go func(procNum int, t chan ExecVersionView) {
		Loop:
			for {
				select {
				case task := <-t:
					{
						res := task.Execute()
						chResults <- res
					}
				case <-chDone:
					break Loop
				}
			}
			println(fmt.Sprintf("proc done %v", procNum)) // TODO: logging ...
		}(i, chTasks)
	}

	mvh := MakeMVHashMap()

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
		switch res.err {
		case nil:
			{
				lastTxIO.recordRead(res.ver.TxnIndex, res.txIn)
				if res.ver.Incarnation == 0 {
					lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				} else {
					if res.txOut.hasNewWrite(lastTxIO.WriteSet(res.ver.TxnIndex)) {
						// TODO: 'if there is a write to a memory location ...'
						// panic("test + implement me")
						println("Revalidate completed txs greater than current tx")
						validateTasks.pushPendingSet(execTasks.getRevalidationRange(res.ver.TxnIndex + 1))
					}

					prevWrite := lastTxIO.WriteSet(res.ver.TxnIndex)

					// Remove entries that were previously written but are no longer written

					cmpMap := make(map[string]bool)

					for _, w := range res.txOut {
						cmpMap[string(w.Path)] = true
					}

					for _, v := range prevWrite {
						if _, ok := cmpMap[string(v.Path)]; !ok {
							mvh.Delete(v.Path, res.ver.TxnIndex)
						}
					}

					lastTxIO.recordWrite(res.ver.TxnIndex, res.txOut)
				}
				validateTasks.pushPending(res.ver.TxnIndex)
				execTasks.markComplete(res.ver.TxnIndex)
				if diagExecSuccess[res.ver.TxnIndex] > 0 && diagExecAbort[res.ver.TxnIndex] == 0 {
					println("got multiple successful execution w/o abort?", res.ver.TxnIndex, res.ver.Incarnation)
				}
				diagExecSuccess[res.ver.TxnIndex]++
				cntSuccess++
			}
		case ErrExecAbort:
			{
				// bit of a subtle / tricky bug here. this adds the tx back to pending ...
				execTasks.revertInProgress(res.ver.TxnIndex)
				// ... but the incarnation needs to be bumped
				txIncarnations[res.ver.TxnIndex]++
				diagExecAbort[res.ver.TxnIndex]++
				cntAbort++
			}
		default:
			{
				err = res.err
				break
			}
		}

		// if we got more work, queue one up...
		nextTx := execTasks.takeNextPending()
		if nextTx != -1 {
			cntExec++
			chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh}
		}

		// do validations ...
		maxComplete := execTasks.maxAllComplete()

		const validationIncrement = 5
		cntValidate := validateTasks.countPending()
		// if we're currently done with all execution tasks then let's validate everything; otherwise do one increment ...
		if execTasks.countComplete() != len(tasks) && cntValidate > validationIncrement {
			cntValidate = validationIncrement
		}
		var toValidate []int
		for i := 0; i < cntValidate; i++ {
			if validateTasks.minPending() <= maxComplete {
				toValidate = append(toValidate, validateTasks.takeNextPending())
			} else {
				break
			}
		}

		for i := 0; i < len(toValidate); i++ {
			cntTotalValidations++
			tx := toValidate[i]
			if ValidateVersion(tx, lastTxIO, mvh) {
				println(fmt.Sprintf("* completed validation task %v", tx))
				validateTasks.markComplete(tx)
			} else {
				println(fmt.Sprintf("* validation task FAILED %v", tx))
				cntValidationFail++
				diagExecAbort[tx]++
				for _, v := range lastTxIO.WriteSet(tx) {
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
		if nextTx == -1 {
			nextTx = execTasks.takeNextPending()
			if nextTx != -1 {
				cntExec++
				chTasks <- ExecVersionView{ver: Version{nextTx, txIncarnations[nextTx]}, et: tasks[nextTx], mvh: mvh}
			}
		}

		if validateTasks.countComplete() == len(tasks) && execTasks.countComplete() == len(tasks) {
			println(fmt.Sprintf("exec summary: %v execs: %v success, %v aborts; %v validations: %v failures",
				cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail))
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
