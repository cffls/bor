# Delta-Based Balance Reconciliation for BlockSTM

## Status: Implementation in progress (stashed in `git stash`)

## Problem
On Polygon mainnet, 80%+ of block transactions modify the same popular contract
balances (WMATIC 0xe3f18acc, USDT 0x3c499c54, Uniswap pools). In Block-STM,
each balance write creates a serialization point — every tx that reads that
balance after a writer must wait for re-execution. This causes ~100 validation
failures per block, making parallel execution slower than serial.

## Proof of Concept
Skipping balance subpath validation entirely (experiment) reduced VFails from
~100+/block to 0-23/block and achieved 1.5-2.2x speedup across 9 mainnet blocks
with 6 workers. 7/9 blocks produced correct state, 2/9 had mismatches (txs that
branch on exact balance).

## Approach: Aptos-style Delta Tracking

### Architecture
1. `BalanceDeltaMap` — shared concurrent map: addr -> sorted list of (txIdx, add, sub)
2. `AddBalance(addr, amount)` records delta `+amount` instead of MVWrite(bal subpath)
3. `SubBalance(addr, amount)` records delta `-amount`
4. `GetBalance(addr)` computes `trie_base + sum(deltas from tx 0..txIdx-1)`
5. Balance subpath key filtered from FlushMVWriteSet (not in MVHashMap)
6. Balance subpath skipped in validation (deltas are commutative)
7. Settlement applies deltas per-tx to finalStateDB

### Implementation (in git stash)
Files changed:
- `core/blockstm/balance_delta.go` — NEW: BalanceDeltaMap
- `core/blockstm/mvhashmap.go` — BalanceDeltas field, validation skip
- `core/state/statedb.go` — GetBalance delta path, AddBalance/SubBalance delta
  recording, FlushMVWriteSet filter, ApplyMVWriteSet balance skip,
  mvRecordWritten balance fixup
- `core/parallel_state_processor.go` — Settlement delta application,
  ResetTxAll on re-execution

### The Bug
All 9 blocks produce wrong state roots. The wrong roots are consistent and
different from both serial and the earlier "skip subpath validation" experiment.

Root cause analysis:
1. When BalanceDeltas is nil, the code is correct (PASS all 9 blocks)
2. The FlushMVWriteSet filter is the critical change: it removes balance entries
   from the MVHashMap, so MVRead returns MVReadResultNone for balance keys
3. GetBalance in delta mode reads `trie_base + accumulated_deltas` — this is
   mathematically correct
4. BUT: `mvRecordWritten` deep-copies a stateObject from a prior tx. That
   stateObject's Balance() is wrong (stale). The fixup code sets it to
   `trie_base + accumulated_deltas`. Then `stateObject.AddBalance(amount)` adds
   to this fixup'd value. This is correct for the WORKER's local state.
5. During settlement, the balance subpath is skipped in ApplyMVWriteSet.
   Instead, deltas from BalanceDeltaMap are applied via AddBalance/SubBalance
   on the finalStateDB.

The suspected issue: the stateObject's balance (after fixup + mutations) and the
delta recorded in BalanceDeltaMap are inconsistent. The worker's stateObject has
`fixup_balance + local_add - local_sub`, while the delta records `local_add` and
`local_sub` separately. During settlement, applying `+local_add` and `-local_sub`
to the finalStateDB should give the same net effect. But the fixup balance
includes `accumulated_deltas` which are from PRIOR txs — those should NOT be
re-applied during settlement (they were already settled by prior txs).

### Next Steps

1. Write a targeted test: 2-3 txs from the same block that share a balance
   dependency. Trace the exact balance at each step (serial vs parallel).

2. The issue may be that settlement applies deltas for ALL addresses that
   appear in the BalanceDeltaMap, but some of those deltas are from addresses
   that the tx's stateObject already handled via the normal ApplyMVWriteSet path
   (non-balance-related writes).

3. Alternative simpler approach: instead of the full delta map, just change
   GetBalance to read `trie_base + accumulated_deltas_from_settlement_so_far`
   where the accumulation happens in the settlement path. Workers that haven't
   settled yet read the base state + settled deltas. This avoids the concurrent
   DeltaMap entirely, but requires settlement to be ahead of execution.

## Test Infrastructure
The following tests are available (added in mainnet_witness_benchmark_test.go):
- `TestMainnetConflictAnalysis` — shows which keys cause conflicts
- `TestMainnetDeltaFeasibility` — proves all balance ops are commutative
- `TestMainnetSerialVsParallel` — per-block timing comparison
- `TestMainnetOpcodeMetrics` — VFails, suspensions, aborts per block
- `TestMainnetWitnessConsistency` — correctness verification (state root match)

## Key Numbers (from experiments)

| Approach | VFails | Speedup | Correct |
|---|---|---|---|
| Baseline (no delta) | ~100+/block | 0.8-1.1x | Yes |
| Skip addr+subpath validation | 0-23/block | 1.5-2.2x | NO (2/9 wrong) |
| Full delta (v2, buggy) | ~0/block | untested | NO (all 9 wrong) |
