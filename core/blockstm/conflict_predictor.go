package blockstm

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// ConflictPredictor learns which nested contract addresses cause conflicts
// when transactions target specific top-level contracts (msg.To). It builds
// a mapping from msg.To → set of conflict addresses from historical blocks.
//
// Before block execution, the predictor is queried to pre-write FlagEstimate
// entries in the MVHashMap, so that MVRead detects the dependency on the first
// execution and suspends the goroutine instead of reading stale data.
type ConflictPredictor struct {
	mu sync.RWMutex

	// msg.To → conflict_address → observation count
	mappings map[common.Address]map[common.Address]uint32

	// Number of blocks processed since last decay
	blocksSinceDecay uint64

	// Minimum observation count for a mapping to be considered "hot"
	threshold uint32

	// Decay interval in blocks
	decayInterval uint64
}

func NewConflictPredictor() *ConflictPredictor {
	return &ConflictPredictor{
		mappings:      make(map[common.Address]map[common.Address]uint32),
		threshold:     2,
		decayInterval: 128,
	}
}

// Record adds a (msg.To → conflict_address) observation.
// Called after block execution with data from read/write set analysis.
func (p *ConflictPredictor) Record(msgTo, conflictAddr common.Address) {
	if msgTo == conflictAddr {
		return // Skip self-references — not useful for prediction
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mappings[msgTo] == nil {
		p.mappings[msgTo] = make(map[common.Address]uint32)
	}

	p.mappings[msgTo][conflictAddr]++
}

// Predict returns the set of addresses that historically conflicted when
// a transaction targets msgTo. Returns nil if no predictions available.
func (p *ConflictPredictor) Predict(msgTo common.Address) []common.Address {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conflicts := p.mappings[msgTo]
	if len(conflicts) == 0 {
		return nil
	}

	result := make([]common.Address, 0, len(conflicts))

	for addr, count := range conflicts {
		if count >= p.threshold {
			result = append(result, addr)
		}
	}

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
		for addr, count := range conflicts {
			newCount := count / 2
			if newCount == 0 {
				delete(conflicts, addr)
			} else {
				conflicts[addr] = newCount
			}
		}

		if len(conflicts) == 0 {
			delete(p.mappings, msgTo)
		}
	}
}
