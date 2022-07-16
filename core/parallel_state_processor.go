// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/blockstm"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type ParallelStateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewParallelStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *ParallelStateProcessor {
	return &ParallelStateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

type ExecutionTask struct {
	msg    types.Message
	config *params.ChainConfig
	bc     ChainContext

	gp          *GasPool
	blockNumber *big.Int
	blockHash   common.Hash
	tx          *types.Transaction
	statedb     *state.StateDB
	evm         *vm.EVM
	result      *ExecutionResult
}

func (task *ExecutionTask) Execute(mvh *blockstm.MVHashMap, incarnation int) error {
	task.statedb.SetMVHashmap(mvh)
	task.statedb.SetIncarnation(incarnation)

	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(task.msg)
	task.evm.Reset(txContext, task.statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(task.evm, task.msg, task.gp)
	if err != nil {
		return fmt.Errorf("could not apply tx %d [%v]: %w", task.statedb.TxIndex(), task.tx.Hash().Hex(), err)
	}

	if task.statedb.HadInvalidRead() {
		return blockstm.ErrExecAbort
	}

	if task.config.IsByzantium(task.blockNumber) {
		task.statedb.Finalise(true)
	} else {
		task.statedb.IntermediateRoot(task.config.IsEIP158(task.blockNumber)).Bytes()
	}

	task.result = result

	return nil
}

func (task *ExecutionTask) MVReadList() []blockstm.ReadDescriptor {
	return task.statedb.MVReadList()
}

func (task *ExecutionTask) MVWriteList() []blockstm.WriteDescriptor {
	return task.statedb.MVWriteList()
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *ParallelStateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		usedGas     = new(uint64)
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	tasks := make([]blockstm.ExecTask, 0, len(block.Transactions()))

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		cleanStatedb := statedb.Copy()

		blockContext := NewEVMBlockContext(header, p.bc, nil)

		cleanStatedb.Prepare(tx.Hash(), i)

		task := &ExecutionTask{
			msg:         msg,
			config:      p.config,
			bc:          p.bc,
			gp:          new(GasPool).AddGas(block.GasLimit()),
			blockNumber: blockNumber,
			blockHash:   blockHash,
			tx:          tx,
			statedb:     cleanStatedb,
			evm:         vm.NewEVM(blockContext, vm.TxContext{}, cleanStatedb, p.config, cfg),
		}

		tasks = append(tasks, task)
	}

	lastTxIO, err := blockstm.ExecuteParallel(tasks)
	if err != nil {
		return nil, nil, 0, err
	}

	for i, task := range tasks {
		task := task.(*ExecutionTask)
		statedb.ApplyMVWriteSet(lastTxIO.WriteSet(i))

		// Update the state with pending changes.
		var root []byte
		if p.config.IsByzantium(blockNumber) {
			statedb.Finalise(true)
		} else {
			root = statedb.IntermediateRoot(p.config.IsEIP158(blockNumber)).Bytes()
		}
		*usedGas += task.result.UsedGas

		// Create a new receipt for the transaction, storing the intermediate root and gas used
		// by the tx.
		receipt := &types.Receipt{Type: task.tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
		if task.result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = task.tx.Hash()
		receipt.GasUsed = task.result.UsedGas

		// If the transaction created a contract, store the creation address in the receipt.
		if task.msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(task.evm.TxContext.Origin, task.tx.Nonce())
		}

		// Set the receipt logs and create the bloom filter.
		receipt.Logs = statedb.GetLogs(task.tx.Hash(), blockHash)
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockHash = blockHash
		receipt.BlockNumber = blockNumber
		receipt.TransactionIndex = uint(statedb.TxIndex())

		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())

	return receipts, allLogs, *usedGas, nil
}
