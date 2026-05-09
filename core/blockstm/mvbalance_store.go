package blockstm

import (
	"sort"
	"sync"

	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/common"
)

// ---------------------------------------------------------------------------
// MVBalanceStore — commutative delta store for balance
// ---------------------------------------------------------------------------

const (
	mvBalanceShards = 64
	// initialMVBalanceShardCap is the per-shard map capacity hint —
	// distinct from mvBalanceShards. Balance deltas are sparser than
	// MVStore entries (only addresses with explicit AddBalance / SubBalance
	// in a tx land here), so 16 starting capacity per shard is enough to
	// avoid early growth without over-allocating.
	initialMVBalanceShardCap = 16
)

type BalanceDelta struct {
	TxIdx int
	Add   uint256.Int
	Sub   uint256.Int
}

type mvBalanceShard struct {
	mu      sync.RWMutex
	data    map[common.Address][]BalanceDelta
	version map[common.Address]uint64 // incremented on every write, for fast validation
}

type MVBalanceStore struct {
	shards [mvBalanceShards]mvBalanceShard
}

func NewMVBalanceStore() *MVBalanceStore {
	s := &MVBalanceStore{}
	for i := range s.shards {
		s.shards[i].data = make(map[common.Address][]BalanceDelta, initialMVBalanceShardCap)
		s.shards[i].version = make(map[common.Address]uint64, initialMVBalanceShardCap)
	}
	return s
}

func (s *MVBalanceStore) shard(addr common.Address) *mvBalanceShard {
	h := uint(addr[0])<<8 | uint(addr[1])
	return &s.shards[h%mvBalanceShards]
}

// WriteDelta accumulates add/sub into the (addr, txIdx) entry, creating it
// if necessary. Multiple WriteDelta calls for the same key sum together.
func (s *MVBalanceStore) WriteDelta(addr common.Address, txIdx int, add, sub *uint256.Int) {
	sh := s.shard(addr)
	sh.mu.Lock()
	entries := sh.data[addr]
	pos := sort.Search(len(entries), func(i int) bool { return entries[i].TxIdx >= txIdx })

	var a, su uint256.Int
	if add != nil {
		a.Set(add)
	}
	if sub != nil {
		su.Set(sub)
	}

	if pos < len(entries) && entries[pos].TxIdx == txIdx {
		entries[pos].Add.Add(&entries[pos].Add, &a)
		entries[pos].Sub.Add(&entries[pos].Sub, &su)
	} else {
		entries = append(entries, BalanceDelta{})
		copy(entries[pos+1:], entries[pos:])
		entries[pos] = BalanceDelta{TxIdx: txIdx, Add: a, Sub: su}
		sh.data[addr] = entries
	}
	sh.version[addr]++
	sh.mu.Unlock()
}

// Version returns the current version counter for an address (for fast validation).
func (s *MVBalanceStore) Version(addr common.Address) uint64 {
	sh := s.shard(addr)
	sh.mu.RLock()
	v := sh.version[addr]
	sh.mu.RUnlock()
	return v
}

// ReadDelta returns accumulated (add, sub) from entries before txIdx.
func (s *MVBalanceStore) ReadDelta(addr common.Address, txIdx int) (add, sub uint256.Int) {
	sh := s.shard(addr)
	sh.mu.RLock()
	for _, e := range sh.data[addr] {
		if e.TxIdx >= txIdx {
			break
		}
		add.Add(&add, &e.Add)
		sub.Add(&sub, &e.Sub)
	}
	sh.mu.RUnlock()
	return
}

// LastWriter returns the highest txIdx that wrote a balance delta for addr
// before txIdx, or -1 if none.
func (s *MVBalanceStore) LastWriter(addr common.Address, txIdx int) int {
	sh := s.shard(addr)
	sh.mu.RLock()
	last := -1
	for _, e := range sh.data[addr] {
		if e.TxIdx >= txIdx {
			break
		}
		last = e.TxIdx
	}
	sh.mu.RUnlock()
	return last
}

// GetTxDelta returns this specific tx's delta.
func (s *MVBalanceStore) GetTxDelta(addr common.Address, txIdx int) (add, sub uint256.Int, found bool) {
	sh := s.shard(addr)
	sh.mu.RLock()
	entries := sh.data[addr]
	pos := sort.Search(len(entries), func(i int) bool { return entries[i].TxIdx >= txIdx })
	if pos < len(entries) && entries[pos].TxIdx == txIdx {
		add.Set(&entries[pos].Add)
		sub.Set(&entries[pos].Sub)
		found = true
	}
	sh.mu.RUnlock()
	return
}

// ZeroDelta resets the delta for txIdx to zero but keeps the entry.
// This allows LastWriter to still find the txIdx (for waitForTx during
// parallel re-execution), while WriteDelta accumulates correctly from zero.
//
// Version is only bumped when an entry actually existed for txIdx —
// no-op zeroing on an absent entry must not invalidate any version-based
// caches downstream.
func (s *MVBalanceStore) ZeroDelta(txIdx int, addrs []common.Address) {
	for _, addr := range addrs {
		sh := s.shard(addr)
		sh.mu.Lock()
		entries := sh.data[addr]
		pos := sort.Search(len(entries), func(i int) bool { return entries[i].TxIdx >= txIdx })
		if pos < len(entries) && entries[pos].TxIdx == txIdx {
			entries[pos].Add.Clear()
			entries[pos].Sub.Clear()
			sh.version[addr]++
		}
		sh.mu.Unlock()
	}
}

// DeleteSingle removes the balance delta for a specific (txIdx, addr).
func (s *MVBalanceStore) DeleteSingle(addr common.Address, txIdx int) {
	sh := s.shard(addr)
	sh.mu.Lock()
	entries := sh.data[addr]
	pos := sort.Search(len(entries), func(i int) bool { return entries[i].TxIdx >= txIdx })
	if pos < len(entries) && entries[pos].TxIdx == txIdx {
		entries = append(entries[:pos], entries[pos+1:]...)
		sh.data[addr] = entries
		sh.version[addr]++
	}
	sh.mu.Unlock()
}
