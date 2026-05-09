package blockstm

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func mvKey(addr byte) Key { return NewAddressKey(common.Address{addr}) }

// TestMVStore_WriteRead covers the happy path: write a value at txIdx=2 and
// observe it from a later reader. Readers before the writer see nothing.
func TestMVStore_WriteRead(t *testing.T) {
	s := NewMVStore()
	k := mvKey(1)

	if _, found := s.Read(k, 10); found {
		t.Fatalf("expected no entry before any write")
	}
	s.WriteInc(k, 2, 0, uint64(42))

	v, found := s.Read(k, 10)
	if !found || v.(uint64) != 42 {
		t.Fatalf("Read at 10: got (%v, %v), want (42, true)", v, found)
	}

	// Reader at txIdx==writer cannot see its own write.
	if _, found := s.Read(k, 2); found {
		t.Fatalf("reader at writer's txIdx must not see the write")
	}
	// Reader at txIdx<writer sees nothing.
	if _, found := s.Read(k, 1); found {
		t.Fatalf("reader before writer must see nothing")
	}
}

// TestMVStore_WriteInc verifies incarnation tracking: the latest write at
// the same txIdx replaces the prior version in place.
func TestMVStore_WriteInc(t *testing.T) {
	s := NewMVStore()
	k := mvKey(1)

	s.WriteInc(k, 2, 0, uint64(10))
	s.WriteInc(k, 2, 1, uint64(20))

	v, writer, inc, found, _ := s.ReadVersionFull(k, 5)
	if !found || v.(uint64) != 20 || writer != 2 || inc != 1 {
		t.Fatalf("ReadVersionFull: got (%v, %d, %d, %v), want (20, 2, 1, true)", v, writer, inc, found)
	}
}

// TestMVStore_ReadVersionFull pins the committed-entry case (estimate=false)
// and the post-MarkEstimate case (estimate=true). The legacy provisional
// flag was removed alongside WriteProvisional.
func TestMVStore_ReadVersionFull(t *testing.T) {
	s := NewMVStore()
	k := mvKey(1)
	s.WriteInc(k, 3, 0, uint64(7))

	_, _, _, found, est := s.ReadVersionFull(k, 10)
	if !found || est {
		t.Fatalf("committed entry: found=%v est=%v; want (true,false)", found, est)
	}

	// MarkEstimate flips the flag.
	s.MarkEstimate(3, []Key{k})
	_, _, _, _, est = s.ReadVersionFull(k, 10)
	if !est {
		t.Fatalf("estimate-flagged entry: est=%v; want true", est)
	}
}

// TestMVStore_Delete removes a specific (key,txIdx) entry.
func TestMVStore_Delete(t *testing.T) {
	s := NewMVStore()
	k := mvKey(1)
	s.WriteInc(k, 2, 0, uint64(1))
	s.WriteInc(k, 4, 0, uint64(2))

	s.Delete(k, 2)

	// Reader at 10 now sees only tx 4.
	v, writer, _, found, _ := s.ReadVersionFull(k, 10)
	if !found || v.(uint64) != 2 || writer != 4 {
		t.Fatalf("after delete: got (%v, %d, _, %v), want (2, 4, true)", v, writer, found)
	}
	// Deleting a missing txIdx is a no-op.
	s.Delete(k, 99)
}

// TestMVStore_EstimateLifecycle covers MarkEstimate + CleanupEstimate.
// After MarkEstimate, ReadVersionFull reports estimate=true; CleanupEstimate
// removes entries still flagged estimate, but not ones that have since been
// re-written (i.e., WriteInc clears the estimate flag).
func TestMVStore_EstimateLifecycle(t *testing.T) {
	s := NewMVStore()
	keep := mvKey(1)
	drop := mvKey(2)
	s.WriteInc(keep, 2, 0, uint64(1))
	s.WriteInc(drop, 2, 0, uint64(2))

	s.MarkEstimate(2, []Key{keep, drop})
	if !s.IsEstimate(keep, 2) || !s.IsEstimate(drop, 2) {
		t.Fatalf("MarkEstimate: flags not set")
	}

	// Re-execute: writer overwrites `keep` at new incarnation, but not `drop`.
	s.WriteInc(keep, 2, 1, uint64(11))
	s.CleanupEstimate(2, []Key{keep, drop})

	v, writer, inc, found, _ := s.ReadVersionFull(keep, 5)
	if !found || v.(uint64) != 11 || writer != 2 || inc != 1 {
		t.Fatalf("keep after cleanup: got (%v, %d, %d, %v), want (11,2,1,true)", v, writer, inc, found)
	}
	if _, found := s.Read(drop, 5); found {
		t.Fatalf("drop after cleanup: expected entry removed (was still estimate)")
	}
	// IsEstimate on missing entry is false.
	if s.IsEstimate(mvKey(99), 2) {
		t.Fatalf("IsEstimate on missing entry must be false")
	}
}

// TestMVStore_BloomFastPath sanity-checks that unseen keys bypass the shard
// lock — verified indirectly by returning not-found on keys never written.
func TestMVStore_BloomFastPath(t *testing.T) {
	s := NewMVStore()
	s.WriteInc(mvKey(1), 2, 0, uint64(1))

	// Different key: bloom filter should miss.
	if _, found := s.Read(mvKey(200), 10); found {
		t.Fatalf("Read on unwritten key returned found=true")
	}
	if _, _, _, found, _ := s.ReadVersionFull(mvKey(200), 10); found {
		t.Fatalf("ReadVersionFull on unwritten key returned found=true")
	}
}
