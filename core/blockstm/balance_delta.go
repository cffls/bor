package blockstm

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// BalanceDelta represents a signed balance change for one tx.
type BalanceDelta struct {
	TxIdx int
	Add   uint256.Int // amount added
	Sub   uint256.Int // amount subtracted
}

// BalanceDeltaMap tracks per-address balance deltas across all txs in a block.
// It is shared across all parallel workers and uses sharded locking for
// concurrent access.
//
// Instead of storing absolute balances in the MVHashMap (which creates false
// serialization when many txs touch the same contract), each AddBalance/SubBalance
// records a delta here. GetBalance computes base + sum(deltas from prior txs).
type BalanceDeltaMap struct {
	shards [64]deltaMapShard
}

type deltaMapShard struct {
	mu      sync.RWMutex
	entries map[common.Address][]BalanceDelta
}

func NewBalanceDeltaMap() *BalanceDeltaMap {
	m := &BalanceDeltaMap{}
	for i := range m.shards {
		m.shards[i].entries = make(map[common.Address][]BalanceDelta)
	}

	return m
}

func (m *BalanceDeltaMap) shard(addr common.Address) *deltaMapShard {
	return &m.shards[uint(addr[0])%64]
}

// RecordAdd records a balance credit for addr at txIdx.
func (m *BalanceDeltaMap) RecordAdd(addr common.Address, txIdx int, amount *uint256.Int) {
	if amount.IsZero() {
		return
	}

	s := m.shard(addr)
	s.mu.Lock()
	defer s.mu.Unlock()

	deltas := s.entries[addr]

	// Check if this txIdx already has an entry (re-execution / incarnation)
	for i := range deltas {
		if deltas[i].TxIdx == txIdx {
			deltas[i].Add.Add(&deltas[i].Add, amount)
			return
		}
	}

	// New entry
	d := BalanceDelta{TxIdx: txIdx}
	d.Add.Set(amount)
	s.entries[addr] = append(deltas, d)
}

// RecordSub records a balance debit for addr at txIdx.
func (m *BalanceDeltaMap) RecordSub(addr common.Address, txIdx int, amount *uint256.Int) {
	if amount.IsZero() {
		return
	}

	s := m.shard(addr)
	s.mu.Lock()
	defer s.mu.Unlock()

	deltas := s.entries[addr]

	for i := range deltas {
		if deltas[i].TxIdx == txIdx {
			deltas[i].Sub.Add(&deltas[i].Sub, amount)
			return
		}
	}

	d := BalanceDelta{TxIdx: txIdx}
	d.Sub.Set(amount)
	s.entries[addr] = append(deltas, d)
}

// ResetTxAll clears all deltas for a specific txIdx across all addresses.
// Called before re-execution to ensure stale deltas from a prior incarnation
// don't accumulate.
func (m *BalanceDeltaMap) ResetTxAll(txIdx int) {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()

		for addr, deltas := range s.entries {
			for j := range deltas {
				if deltas[j].TxIdx == txIdx {
					deltas[j].Add.Clear()
					deltas[j].Sub.Clear()
				}
			}
			s.entries[addr] = deltas
		}

		s.mu.Unlock()
	}
}

// AccumulatedDelta returns the net accumulated balance change from txs
// [0, txIdx) for the given address. The result is (totalAdd, totalSub).
func (m *BalanceDeltaMap) AccumulatedDelta(addr common.Address, txIdx int) (totalAdd uint256.Int, totalSub uint256.Int) {
	s := m.shard(addr)
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, d := range s.entries[addr] {
		if d.TxIdx < txIdx {
			totalAdd.Add(&totalAdd, &d.Add)
			totalSub.Add(&totalSub, &d.Sub)
		}
	}

	return
}

// TxDelta returns the delta for a specific tx.
func (m *BalanceDeltaMap) TxDelta(addr common.Address, txIdx int) (add uint256.Int, sub uint256.Int) {
	s := m.shard(addr)
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, d := range s.entries[addr] {
		if d.TxIdx == txIdx {
			add.Set(&d.Add)
			sub.Set(&d.Sub)
			return
		}
	}

	return
}

// AllAddresses returns all addresses that have deltas.
func (m *BalanceDeltaMap) AllAddresses() []common.Address {
	var addrs []common.Address

	for i := range m.shards {
		m.shards[i].mu.RLock()
		for addr := range m.shards[i].entries {
			addrs = append(addrs, addr)
		}
		m.shards[i].mu.RUnlock()
	}

	return addrs
}
