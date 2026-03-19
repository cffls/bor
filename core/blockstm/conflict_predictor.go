package blockstm

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// ConflictPredictor learns which MVHashMap keys cause conflicts when
// transactions target specific top-level contracts (msg.To). It builds
// a mapping from msg.To → set of conflict keys from historical blocks.
//
// Keys are stored at full granularity (address + storage slot hash + type),
// so predictions target the exact storage slots that conflict — not just
// the contract address. Address-type keys (structural dependencies from
// getStateObject) are filtered out during recording since they don't
// represent true data dependencies.
//
// Before block execution, the predictor is queried to pre-write FlagEstimate
// entries in the MVHashMap, so that MVRead detects the dependency on the first
// execution and suspends the goroutine instead of reading stale data.
type ConflictPredictor struct {
	mu sync.RWMutex

	// msg.To → conflict_key → observation count
	mappings map[common.Address]map[Key]uint32

	// Number of blocks processed since last decay
	blocksSinceDecay uint64

	// Minimum observation count for a mapping to be considered "hot"
	threshold uint32

	// Decay interval in blocks
	decayInterval uint64

	// Cached predictions (invalidated on Record/EndBlock)
	predCache map[common.Address][]Key
}

func NewConflictPredictor() *ConflictPredictor {
	return &ConflictPredictor{
		mappings:      make(map[common.Address]map[Key]uint32),
		threshold:     2,
		decayInterval: 128,
	}
}

// Record adds a (msg.To → conflict_key) observation.
// Called after block execution with data from read/write set analysis.
// Address-type keys are skipped — they are structural dependencies from
// getStateObject(), not real data conflicts. State keys (storage slots)
// and subpath keys (balance, nonce, code) are recorded.
func (p *ConflictPredictor) Record(msgTo common.Address, conflictKey Key) {
	// Skip address-type keys — every tx that touches a contract reads its
	// address key via getStateObject, creating false positives.
	if conflictKey.IsAddress() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.predCache = nil // invalidate cache

	if p.mappings[msgTo] == nil {
		p.mappings[msgTo] = make(map[Key]uint32)
	}

	p.mappings[msgTo][conflictKey]++
}

// Predict returns the set of keys that historically conflicted when
// a transaction targets msgTo. Returns nil if no predictions available.
func (p *ConflictPredictor) Predict(msgTo common.Address) []Key {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check cache first
	if p.predCache != nil {
		if cached, ok := p.predCache[msgTo]; ok {
			return cached
		}
	}

	conflicts := p.mappings[msgTo]
	if len(conflicts) == 0 {
		return nil
	}

	result := make([]Key, 0, len(conflicts))

	for key, count := range conflicts {
		if count >= p.threshold {
			result = append(result, key)
		}
	}

	// Cache the result
	if p.predCache == nil {
		p.predCache = make(map[common.Address][]Key)
	}

	p.predCache[msgTo] = result

	return result
}

// EndBlock should be called after each block is processed. It increments
// the block counter and triggers decay when the interval is reached.
func (p *ConflictPredictor) EndBlock() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.blocksSinceDecay++

	if p.blocksSinceDecay >= p.decayInterval {
		p.blocksSinceDecay = 0
		p.decay()
	}
}

// decay halves all counts and removes entries that drop to zero.
// Must be called with mu held.
func (p *ConflictPredictor) decay() {
	for msgTo, conflicts := range p.mappings {
		for key, count := range conflicts {
			newCount := count / 2
			if newCount == 0 {
				delete(conflicts, key)
			} else {
				conflicts[key] = newCount
			}
		}

		if len(conflicts) == 0 {
			delete(p.mappings, msgTo)
		}
	}
}
