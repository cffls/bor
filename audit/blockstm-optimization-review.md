# BlockSTM Optimization Review

Review of uncommitted changes in `git diff` touching BlockSTM-related code.

**Files changed:**
- `core/blockstm/mvhashmap.go`
- `core/blockstm/mvhashmap_test.go`
- `core/parallel_state_processor.go`
- `core/state/statedb.go`
- `core/state/statedb_test.go`
- `eth/tracers/data.csv`
- `miner/worker.go`
- `miner/worker_test.go`

---

## 1. `KeyHash` — Smaller Map Keys (`mvhashmap.go`)

**What:** Introduces `KeyHash uint64` (an FNV-based hash of the 54-byte `Key`) and changes all read/write maps from `map[Key]...` to `map[KeyHash]...`.

**Why it helps:** Go's map hashing is proportional to key size. A `[54]byte` key requires hashing 54 bytes on every lookup; a `uint64` hashes in a single instruction. This affects every `MVRead` and `MVWrite`, which are hot paths.

**Concern — hash collisions (correctness risk):** The `Key` type is 54 bytes of structured data (address + hash + subpath + type). FNV-64 has a non-negligible collision probability over large key spaces. A collision between two distinct keys would cause one tx to incorrectly see another tx's write as a cache hit and skip the MVHashMap read — producing **wrong state**. The change doesn't document this trade-off or add any collision assertion. Using the full `Key` as a map key was collision-free by definition.

---

## 2. `treemap` → sorted `[]txnEntry` slice (`mvhashmap.go`)

**What:** Replaces `github.com/emirpasic/gods/maps/treemap` (a red-black tree) with a sorted `[]txnEntry` slice backed by `sort.Search` (binary search) for the per-key transaction index map inside `TxnIndexCells`.

**Why it helps:** For typical block sizes (100–300 txs), a flat slice with binary search has much better cache locality than a pointer-chased red-black tree. The `floor` query (largest index ≤ `txIdx - 1`) is now a single `sort.Search` call.

**Concern — `Delete` is O(N):** The slice `Delete` does a `copy` to shift elements, which is O(N). The old tree `Remove` was O(log N). If `MarkEstimate`/`Delete` is called frequently — and it is, on every execution abort — this could regress under high re-execution rates. This trade-off is not documented. The abort-heavy case should be benchmarked explicitly.

---

## 3. `writeAddrs` two-level filter (`statedb.go`)

**What:** Adds a `map[common.Address]struct{}` (`writeAddrs`) alongside `writeMap`. In `MVRead`, before the full `writeMap[kh]` lookup, it first checks `writeAddrs[addr]` as a fast negative filter.

**Why it helps:** Most reads are for addresses that the current tx hasn't written to. The address-level filter short-circuits the lookup cheaply when the address has no writes at all.

**Concern — diminished benefit after `KeyHash`:** Since `writeMap` now uses a `uint64` key (after change 1), each lookup is already cheap. The `writeAddrs` check adds an unconditional extra map lookup before every read. The net benefit is real only when the tx has written to *some* addresses but not the one being read. In practice still likely positive, but smaller than it would have been with the original 54-byte key.

---

## 4. `ApplyMVWriteSet` — stateObject cache + direct field access (`statedb.go`)

**What:** Adds an `objCache map[common.Address]objPair` inside `ApplyMVWriteSet` to avoid repeated `getOrNewStateObject` calls per address. Also replaces `sr.GetBalance(addr)`, `sr.GetNonce(addr)`, etc. with direct field accessors `pair.src.Balance()`, `pair.src.Nonce()`, etc.

**Why it helps:** During settlement, multiple writes for the same address (balance + nonce + N storage slots) previously each triggered a full `getOrNewStateObject` lookup. The cache makes subsequent accesses O(1).

**Concern — nil `pair.src` silently drops writes:** If `pair.src` (the worker stateObject) is nil, the balance/nonce/code write is silently skipped. Previously `sr.GetBalance(addr)` would return the zero value and `SetBalance` would still execute. Now the `Set*` call is omitted entirely. This could occur if `Finalise` was not called on the worker statedb before `ApplyMVWriteSet`, leaving `stateObjects` partially populated. The code doesn't document when `pair.src` can be nil or assert it cannot be.

---

## 5. `Settle()` — disable MVHashMap during settlement (`parallel_state_processor.go`)

**What:** Temporarily sets `finalStateDB`'s MVHashMap to `nil` at the start of `Settle()` and restores it at the end. This makes all `Get*`/`Set*` calls inside `Settle` bypass `MVRead`/`MVWrite` overhead.

**Why it helps:** Settlement is sequential (transactions are committed in order), so the multiversion overhead is unnecessary and purely wasted work.

**Concern — unsynchronized access:** `SetMVHashmap(nil)` and the subsequent restore are not guarded by any lock. If any other goroutine (e.g. the BlockSTM scheduler) accesses `finalStateDB.mvHashmap` concurrently during settlement, this is a **data race**. This needs confirmation that `Settle()` is always called in a context where no other goroutine can concurrently access `finalStateDB`.

---

## 6. `stateObjects` cache shortcut in `MVRead` (`statedb.go`)

**What:** Changes the deleted-account guard from always calling `getStateObject(addr)` to first checking `s.stateObjects[addr] != nil` and only calling `getStateObject` on a miss.

**Why it helps:** `getStateObject` triggers a recursive MVRead for the account's address key. Checking the local cache first avoids that recursive call when the address was already accessed in the same tx.

**Concern — fragile invariant:** The comment states "Each execution starts with empty `stateObjects` (from `cleanStateDB.Copy()`), so non-nil means we've already verified this address via MVRead." This assumption breaks if `stateObjects` is ever pre-populated before execution begins (e.g. via pre-warming). If that invariant is violated, the guard could be skipped for a legitimately deleted account, leading to reads from a destroyed stateObject.

---

## Minor / Cosmetic

- `GetAddress()` and `GetStateKey()` changed to named return + `copy` instead of `common.BytesToAddress/Hash`. Correct — avoids a heap allocation.
- `NewAddressKey` and `NewSubpathKey` inlined to skip the zero-hash path in `newKey`. Clean.
- `data.csv` row reordering is a side-effect of map iteration order changing because `KeyHash` replaced `Key` in the read/write maps (Go map iteration order is non-deterministic).

---

## Summary

| Change | Correctness Risk | Performance Benefit |
|---|---|---|
| `KeyHash` for map keys | **Hash collision → wrong state** | High — hot path |
| Sorted slice vs treemap | `Delete` O(N) under high abort rate | High — cache locality |
| `writeAddrs` address filter | Low | Medium |
| `ApplyMVWriteSet` objCache | Silent write drop on nil src | Medium |
| Settle: null MVHashMap | Potential data race | Low–medium |
| `stateObjects` cache in MVRead | Fragile invariant assumption | Low–medium |

The most significant concern is the `KeyHash` collision risk. All other changes are sound in direction but several need defensive comments, assertions, or documented invariants before merging.
