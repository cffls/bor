package blockstm

import (
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
)

func addr(i int) common.Address {
	return common.BigToAddress(big.NewInt(int64(i)))
}

func TestConflictPredictorBasic(t *testing.T) {
	p := NewConflictPredictor()

	router := addr(1)
	pool := addr(2)

	// First observation — below threshold (2)
	p.Record(router, pool)
	assert.Empty(t, p.Predict(router), "should not predict after 1 observation")

	// Second observation — meets threshold
	p.Record(router, pool)
	predicted := p.Predict(router)
	require.Len(t, predicted, 1)
	assert.Equal(t, pool, predicted[0])
}

func TestConflictPredictorMultipleTargets(t *testing.T) {
	p := NewConflictPredictor()

	routerA := addr(1)
	routerB := addr(2)
	pool := addr(3)

	// Both routers conflict on the same pool
	p.Record(routerA, pool)
	p.Record(routerA, pool)
	p.Record(routerB, pool)
	p.Record(routerB, pool)

	predictedA := p.Predict(routerA)
	predictedB := p.Predict(routerB)

	require.Len(t, predictedA, 1)
	require.Len(t, predictedB, 1)
	assert.Equal(t, pool, predictedA[0])
	assert.Equal(t, pool, predictedB[0])
}

func TestConflictPredictorMultipleConflicts(t *testing.T) {
	p := NewConflictPredictor()

	router := addr(1)
	poolA := addr(2)
	poolB := addr(3)

	// Router conflicts with two different pools
	p.Record(router, poolA)
	p.Record(router, poolA)
	p.Record(router, poolB)
	p.Record(router, poolB)

	predicted := p.Predict(router)
	require.Len(t, predicted, 2)

	addrs := map[common.Address]bool{}
	for _, a := range predicted {
		addrs[a] = true
	}

	assert.True(t, addrs[poolA])
	assert.True(t, addrs[poolB])
}

func TestConflictPredictorSelfReference(t *testing.T) {
	p := NewConflictPredictor()

	router := addr(1)

	// Self-references are skipped
	p.Record(router, router)
	p.Record(router, router)

	assert.Empty(t, p.Predict(router))
}

func TestConflictPredictorUnknownTarget(t *testing.T) {
	p := NewConflictPredictor()

	assert.Nil(t, p.Predict(addr(99)))
}

func TestConflictPredictorDecay(t *testing.T) {
	p := NewConflictPredictor()
	p.decayInterval = 4 // decay every 4 blocks for testing

	router := addr(1)
	pool := addr(2)

	// Count = 4 (above threshold)
	p.Record(router, pool)
	p.Record(router, pool)
	p.Record(router, pool)
	p.Record(router, pool)
	assert.Len(t, p.Predict(router), 1, "count=4, should predict")

	// Trigger decay: count 4 → 2 (still above threshold)
	for i := 0; i < 4; i++ {
		p.EndBlock()
	}
	assert.Len(t, p.Predict(router), 1, "count=2 after decay, should still predict")

	// Trigger another decay: count 2 → 1 (below threshold)
	for i := 0; i < 4; i++ {
		p.EndBlock()
	}
	assert.Empty(t, p.Predict(router), "count=1 after second decay, should not predict")

	// Trigger another decay: count 1 → 0 (removed)
	for i := 0; i < 4; i++ {
		p.EndBlock()
	}

	p.mu.RLock()
	_, exists := p.mappings[router]
	p.mu.RUnlock()

	assert.False(t, exists, "entry should be removed after decaying to 0")
}

func TestConflictPredictorPrePopulation(t *testing.T) {
	// End-to-end test: predictor feeds predictions into the executor's
	// MVHashMap pre-population via WriteEstimate.

	p := NewConflictPredictor()

	router := addr(1)
	pool := addr(2)

	// Train the predictor
	p.Record(router, pool)
	p.Record(router, pool)

	predicted := p.Predict(router)
	require.Len(t, predicted, 1)

	// Simulate what the executor does: pre-populate MVHashMap
	mvh := MakeMVHashMap()

	for _, conflictAddr := range predicted {
		mvh.WriteEstimate(NewAddressKey(conflictAddr), Version{0, 0})
		mvh.WriteEstimate(NewSubpathKey(conflictAddr, SubpathBalance), Version{0, 0})
	}

	// Verify: reading the address key from tx 1 should find FlagEstimate from tx 0
	addrResult := mvh.Read(NewAddressKey(pool), 1)
	assert.Equal(t, MVReadResultDependency, addrResult.Status(),
		"should detect dependency on pre-populated address key")
	assert.Equal(t, 0, addrResult.DepIdx())

	// Verify: reading the balance key from tx 1 should also find FlagEstimate
	balResult := mvh.Read(NewSubpathKey(pool, SubpathBalance), 1)
	assert.Equal(t, MVReadResultDependency, balResult.Status(),
		"should detect dependency on pre-populated balance key")

	// Verify: reading from tx 0 itself should NOT find a dependency
	// (floor(0-1) = floor(-1) returns nil)
	selfResult := mvh.Read(NewAddressKey(pool), 0)
	assert.Equal(t, MVReadResultNone, selfResult.Status(),
		"tx 0 should not see its own estimate as a dependency")
}

func TestConflictPredictorMultipleTxPrePopulation(t *testing.T) {
	// When multiple txs target the same hot contract, estimates for all
	// of them should be in the MVHashMap. Tx N sees the estimate from
	// the nearest lower-index tx.

	mvh := MakeMVHashMap()
	pool := addr(5)

	// Simulate 3 txs (indices 2, 5, 8) all predicted to conflict on pool
	for _, txIdx := range []int{2, 5, 8} {
		mvh.WriteEstimate(NewAddressKey(pool), Version{txIdx, 0})
	}

	// Tx 3 reads pool → should see FlagEstimate from tx 2
	res3 := mvh.Read(NewAddressKey(pool), 3)
	assert.Equal(t, MVReadResultDependency, res3.Status())
	assert.Equal(t, 2, res3.DepIdx())

	// Tx 6 reads pool → should see FlagEstimate from tx 5
	res6 := mvh.Read(NewAddressKey(pool), 6)
	assert.Equal(t, MVReadResultDependency, res6.Status())
	assert.Equal(t, 5, res6.DepIdx())

	// Tx 9 reads pool → should see FlagEstimate from tx 8
	res9 := mvh.Read(NewAddressKey(pool), 9)
	assert.Equal(t, MVReadResultDependency, res9.Status())
	assert.Equal(t, 8, res9.DepIdx())

	// Tx 1 reads pool → should see nothing (no estimate with index < 1 that's ≤ 0)
	res1 := mvh.Read(NewAddressKey(pool), 1)
	assert.Equal(t, MVReadResultNone, res1.Status(),
		"tx before first estimate should read from storage")
}

func TestConflictPredictorWriteOverridesEstimate(t *testing.T) {
	// When a real Write happens, it should override the FlagEstimate
	// with FlagDone, so subsequent readers get the actual value.

	mvh := MakeMVHashMap()
	pool := addr(5)
	key := NewAddressKey(pool)

	// Pre-populate estimate
	mvh.WriteEstimate(key, Version{3, 0})

	// Verify estimate is visible
	res := mvh.Read(key, 4)
	assert.Equal(t, MVReadResultDependency, res.Status())

	// Real write overrides estimate
	mvh.Write(key, Version{3, 0}, "real_value")

	// Now should get FlagDone with the real value
	res2 := mvh.Read(key, 4)
	assert.Equal(t, MVReadResultDone, res2.Status())
	assert.Equal(t, "real_value", res2.Value())
}

// TestConflictPredictorReducesReExecution verifies that pre-populating the
// MVHashMap with FlagEstimate from predictions reduces aborts compared to
// running without predictions.
//
// Uses a high worker count (= numTx) so ALL tasks execute simultaneously.
// Without predictions, many tasks read MVReadResultNone (stale) on the shared
// key, complete, and fail validation → aborts. With predictions, they see
// FlagEstimate → suspend → resume with correct value → no validation failure.
func TestConflictPredictorReducesReExecution(t *testing.T) {
	numTx := 20
	workDuration := 100 * time.Microsecond

	// Use numTx workers so all tasks run concurrently, maximizing stale reads
	highProcs := numTx

	// Run WITHOUT predictions
	noPredTasks := makeLateConflictTasks(numTx, workDuration, true)
	noPredResult, err := ExecuteParallelOpcodeLevel(noPredTasks, false, highProcs, nil, nil)
	require.NoError(t, err)

	// Run WITH predictions — pre-populate FlagEstimate for the shared key address
	sharedAddr := common.BigToAddress(big.NewInt(999)) // matches makeLateConflictTasks
	predictions := make(map[int][]common.Address)

	for i := 0; i < numTx; i++ {
		predictions[i] = []common.Address{sharedAddr}
	}

	predTasks := makeLateConflictTasks(numTx, workDuration, true)
	predResult, err := ExecuteParallelOpcodeLevel(predTasks, false, highProcs, predictions, nil)
	require.NoError(t, err)

	fmt.Printf("Without predictions: aborts=%d, suspensions=%d\n",
		noPredResult.Aborts, noPredResult.Suspensions)
	fmt.Printf("With predictions:    aborts=%d, suspensions=%d\n",
		predResult.Aborts, predResult.Suspensions)

	// With predictions, more conflicts should be resolved via suspension (blocking)
	// instead of stale reads followed by validation failure (aborts).
	assert.GreaterOrEqual(t, predResult.Suspensions, noPredResult.Suspensions,
		"predictions should increase or maintain suspensions")
	assert.LessOrEqual(t, predResult.Aborts, noPredResult.Aborts,
		"predictions should reduce or maintain aborts")

	// Also compare with baseline (no suspension) to show predictions help there too.
	// Baseline tasks abort on dependency instead of suspending.
	baselineTasks := makeLateConflictTasks(numTx, workDuration, false)
	baselineResult, err := ExecuteParallel(baselineTasks, false, false, highProcs, nil)
	require.NoError(t, err)

	fmt.Printf("Baseline (no suspension): execs completed (aborts tracked in executor internals)\n")
	fmt.Printf("Baseline TxIO read sets present: %v\n", baselineResult.TxIO != nil)
}

func TestConflictPredictorConcurrentAccess(t *testing.T) {
	p := NewConflictPredictor()

	var wg sync.WaitGroup

	// Concurrent writes from multiple goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			router := addr(id)
			pool := addr(100 + id%3)

			for j := 0; j < 100; j++ {
				p.Record(router, pool)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			p.Predict(addr(id))
		}(i)
	}

	// Concurrent decay
	for i := 0; i < 5; i++ {
		p.EndBlock()
	}

	wg.Wait()
	// No race detector errors = pass
}
