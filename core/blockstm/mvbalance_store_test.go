package blockstm

import (
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/common"
)

func u(n uint64) *uint256.Int { return uint256.NewInt(n) }

func mvBalAddr(b byte) common.Address { return common.Address{b} }

// timeAfter returns a channel that fires after n seconds. Used to put a
// deadline on lock-acquisition tests so a deadlock surfaces as a test
// failure rather than a hang.
func timeAfter(seconds int) <-chan time.Time {
	return time.After(time.Duration(seconds) * time.Second)
}

// TestMVBalanceStore_WriteReadDelta covers basic accumulation: two writes
// by different txs accumulate; a reader at a later txIdx sees the sum of
// all prior deltas.
func TestMVBalanceStore_WriteReadDelta(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)

	s.WriteDelta(addr, 1, u(100), u(10))
	s.WriteDelta(addr, 2, u(50), u(0))

	add, sub := s.ReadDelta(addr, 5)
	if add.Uint64() != 150 || sub.Uint64() != 10 {
		t.Fatalf("ReadDelta at 5: got (add=%d, sub=%d), want (150, 10)", add.Uint64(), sub.Uint64())
	}

	// Reader at txIdx==writer doesn't include the writer's delta.
	add, sub = s.ReadDelta(addr, 1)
	if add.Uint64() != 0 || sub.Uint64() != 0 {
		t.Fatalf("ReadDelta at 1: got (add=%d, sub=%d), want (0, 0)", add.Uint64(), sub.Uint64())
	}
	// Reader at 2 sees only tx 1's delta.
	add, sub = s.ReadDelta(addr, 2)
	if add.Uint64() != 100 || sub.Uint64() != 10 {
		t.Fatalf("ReadDelta at 2: got (%d, %d), want (100, 10)", add.Uint64(), sub.Uint64())
	}
}

// TestMVBalanceStore_WriteDeltaAccumulatesSameTx verifies that multiple
// WriteDelta calls for the same (addr, txIdx) merge additively.
func TestMVBalanceStore_WriteDeltaAccumulatesSameTx(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)

	s.WriteDelta(addr, 2, u(10), u(0))
	s.WriteDelta(addr, 2, u(5), u(3))

	add, sub, found := s.GetTxDelta(addr, 2)
	if !found || add.Uint64() != 15 || sub.Uint64() != 3 {
		t.Fatalf("GetTxDelta: got (add=%d, sub=%d, found=%v), want (15, 3, true)", add.Uint64(), sub.Uint64(), found)
	}
}

// TestMVBalanceStore_GetTxDeltaMissing returns found=false for a missing
// (addr, txIdx).
func TestMVBalanceStore_GetTxDeltaMissing(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 5, u(1), u(0))

	if _, _, found := s.GetTxDelta(addr, 2); found {
		t.Fatalf("GetTxDelta(missing): found=true, want false")
	}
}

// TestMVBalanceStore_LastWriter returns the highest tx index strictly
// less than txIdx that has a delta for addr.
func TestMVBalanceStore_LastWriter(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 1, u(1), u(0))
	s.WriteDelta(addr, 4, u(1), u(0))

	if got := s.LastWriter(addr, 10); got != 4 {
		t.Fatalf("LastWriter(10): got %d, want 4", got)
	}
	if got := s.LastWriter(addr, 4); got != 1 {
		t.Fatalf("LastWriter(4): got %d, want 1", got)
	}
	if got := s.LastWriter(addr, 1); got != -1 {
		t.Fatalf("LastWriter(1): got %d, want -1", got)
	}
	if got := s.LastWriter(mvBalAddr(99), 10); got != -1 {
		t.Fatalf("LastWriter(missing addr): got %d, want -1", got)
	}
}

// TestMVBalanceStore_ZeroDelta preserves the entry but zeros its amounts —
// used when MarkEstimate runs before re-execution.
func TestMVBalanceStore_ZeroDelta(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 2, u(10), u(3))

	s.ZeroDelta(2, []common.Address{addr})

	add, sub, found := s.GetTxDelta(addr, 2)
	if !found || add.Uint64() != 0 || sub.Uint64() != 0 {
		t.Fatalf("after ZeroDelta: got (%d, %d, %v), want (0, 0, true)", add.Uint64(), sub.Uint64(), found)
	}
	// LastWriter still finds the (zeroed) tx — entry retained.
	if got := s.LastWriter(addr, 5); got != 2 {
		t.Fatalf("LastWriter after zero: got %d, want 2 (entry retained)", got)
	}
}

// TestMVBalanceStore_DeleteSingle removes just one (addr, txIdx) entry.
func TestMVBalanceStore_DeleteSingle(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 1, u(1), u(0))
	s.WriteDelta(addr, 3, u(2), u(0))

	s.DeleteSingle(addr, 1)

	add, _ := s.ReadDelta(addr, 10)
	if add.Uint64() != 2 {
		t.Fatalf("ReadDelta after DeleteSingle: got %d, want 2", add.Uint64())
	}
	// Deleting missing entry is a no-op.
	s.DeleteSingle(addr, 99)
}

// TestMVBalanceStore_Version increments on every write-like mutation.
func TestMVBalanceStore_Version(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)

	if got := s.Version(addr); got != 0 {
		t.Fatalf("initial Version: got %d, want 0", got)
	}
	s.WriteDelta(addr, 1, u(1), u(0))
	v1 := s.Version(addr)
	s.WriteDelta(addr, 2, u(1), u(0))
	v2 := s.Version(addr)
	if v2 <= v1 {
		t.Fatalf("Version did not increment on write: %d → %d", v1, v2)
	}
	s.ZeroDelta(1, []common.Address{addr})
	v3 := s.Version(addr)
	if v3 <= v2 {
		t.Fatalf("Version did not increment on ZeroDelta: %d → %d", v2, v3)
	}
}

// TestMVBalanceStore_ZeroDelta_AbsentEntryNoVersionBump pins the fix for
// the spurious-version-bump bug: when ZeroDelta is called on a (txIdx, addr)
// pair that has no entry, it must be a true no-op — including no version
// increment, since downstream cache invalidation keys on Version().
func TestMVBalanceStore_ZeroDelta_AbsentEntryNoVersionBump(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 7, u(5), u(0))
	v0 := s.Version(addr)

	// txIdx=99 has no entry; ZeroDelta must not bump the version.
	s.ZeroDelta(99, []common.Address{addr})
	if got := s.Version(addr); got != v0 {
		t.Fatalf("ZeroDelta on absent entry bumped version: %d → %d", v0, got)
	}

	// And one more sanity: bumping on a present entry still works.
	s.ZeroDelta(7, []common.Address{addr})
	if got := s.Version(addr); got <= v0 {
		t.Fatalf("ZeroDelta on present entry must bump version: %d → %d", v0, got)
	}
}

// TestMVBalanceStore_DeleteSingle_BumpsVersion pins that delete (when it
// finds an entry) advances the version counter so downstream caches
// invalidate. This is what consumers of Version() rely on.
func TestMVBalanceStore_DeleteSingle_BumpsVersion(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 3, u(1), u(0))
	v0 := s.Version(addr)

	s.DeleteSingle(addr, 3)
	if got := s.Version(addr); got <= v0 {
		t.Fatalf("DeleteSingle on present entry must bump version: %d → %d", v0, got)
	}

	// And the no-op path stays no-op.
	v1 := s.Version(addr)
	s.DeleteSingle(addr, 99)
	if got := s.Version(addr); got != v1 {
		t.Fatalf("DeleteSingle on absent entry must not bump version: %d → %d", v1, got)
	}
}

// TestMVBalanceStore_WriteDelta_OutOfOrderInsertion pins the slice-
// insertion `copy(entries[pos+1:], entries[pos:])` shift that surfaces
// when a smaller-indexed write lands after a larger one (forces middle
// insertion instead of append). Without the shift, the older write
// overwrites the newer one and ReadDelta returns a stale sum.
func TestMVBalanceStore_WriteDelta_OutOfOrderInsertion(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	// Larger txIdx first, then smaller — forces middle insertion.
	s.WriteDelta(addr, 10, u(100), nil)
	s.WriteDelta(addr, 3, u(7), nil)

	// ReadDelta(addr, 11) sums entries with TxIdx < 11.
	add, _ := s.ReadDelta(addr, 11)
	if add.Uint64() != 107 {
		t.Fatalf("ReadDelta after out-of-order insertion: got %d, want 107 (3→7 + 10→100)", add.Uint64())
	}

	// And the per-tx entries stay separately addressable.
	a3, _, found3 := s.GetTxDelta(addr, 3)
	if !found3 || a3.Uint64() != 7 {
		t.Fatalf("GetTxDelta(3): got (%d, %v), want (7, true)", a3.Uint64(), found3)
	}
	a10, _, found10 := s.GetTxDelta(addr, 10)
	if !found10 || a10.Uint64() != 100 {
		t.Fatalf("GetTxDelta(10): got (%d, %v), want (100, true)", a10.Uint64(), found10)
	}
}

// TestMVBalanceStore_LastWriter_LockReleased verifies LastWriter's
// RUnlock is reachable on every code path. A subsequent writer would
// deadlock if RUnlock were skipped — test by chaining a writer after
// a reader.
func TestMVBalanceStore_LastWriter_LockReleased(t *testing.T) {
	s := NewMVBalanceStore()
	addr := mvBalAddr(1)
	s.WriteDelta(addr, 1, u(1), nil)

	if got := s.LastWriter(addr, 5); got != 1 {
		t.Fatalf("LastWriter: got %d, want 1", got)
	}
	// Acquire write lock — would deadlock if LastWriter forgot to RUnlock.
	done := make(chan struct{})
	go func() {
		s.WriteDelta(addr, 2, u(2), nil)
		close(done)
	}()
	select {
	case <-done:
	case <-timeAfter(2):
		t.Fatal("WriteDelta after LastWriter timed out — RUnlock missing?")
	}
}
