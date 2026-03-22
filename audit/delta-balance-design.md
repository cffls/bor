# Delta-Based Balance Reconciliation for BlockSTM

## Status: Separate BalanceDeltaMap approach does NOT work. Must integrate into MVHashMap.

## Problem
On Polygon mainnet, 80%+ of block transactions modify the same popular contract
balances (WMATIC 0xe3f18acc, USDT 0x3c499c54, Uniswap pools). In Block-STM,
each balance write creates a serialization point — every tx that reads that
balance after a writer must wait for re-execution. This causes ~100 validation
failures per block, making parallel execution slower than serial.

## Key Finding: Skipping Validation Works but is Unsafe

Skipping address + balance subpath validation (one-line change in ValidateVersion)
produces correct results on 9/9 blocks for baseline parallel executor, with
1.35-2.23x speedup. BUT this is unsafe for block mining because real balance
conflicts (e.g., double-spend) would not be caught.

## Separate BalanceDeltaMap: Why It Doesn't Work

A separate `BalanceDeltaMap` alongside MVHashMap was implemented (in
`core/blockstm/balance_delta.go`). The approach:
1. AddBalance/SubBalance record deltas locally per-tx
2. After execution, flush atomically to shared BalanceDeltaMap
3. GetBalance computes base + accumulated_deltas
4. Validation checks delta version consistency
5. Settlement applies deltas

**Result: Same VFail rate (~100+/block) because the delta validation catches
stale delta snapshots just like the old version-based validation.** The
fundamental issue: when tx N reads `AccumulatedDelta(addr, N)`, it gets a
snapshot that may not include tx M's delta (if M hasn't flushed yet). When
validation runs after M flushes, the delta sum has changed → VFail.

## The Correct Approach: Aptos-style MVHashMap Delta Entries

Aptos solves this by integrating deltas INTO the MVHashMap, not alongside it:
1. Balance writes create `FlagDelta` entries (not `FlagDone`)
2. `MVHashMap.Read` for delta keys accumulates all deltas backwards
3. `FlagEstimate` on a delta entry causes suspension/abort (like any other key)
4. Validation checks delta consistency (accumulated sum matches)
5. Settlement resolves deltas to concrete values

This ensures consistency because:
- A tx that reads a balance BEFORE a prior tx writes its delta sees `FlagEstimate`
  → suspends/aborts → retries after the prior tx completes
- A tx that reads AFTER the prior tx writes sees the accumulated delta → correct
- Validation compares accumulated delta sums, which are stable once all prior txs
  have committed their deltas

### Implementation Requirements

1. Add `FlagDelta` to MVHashMap entry types
2. Modify `Write` to support delta entries (add vs overwrite)
3. Modify `Read` to accumulate deltas backwards until hitting a concrete value
4. Modify `MarkEstimate` for delta entries
5. Modify `ValidateVersion` for delta entries
6. Modify `GetBalance`/`AddBalance`/`SubBalance` to use delta writes
7. Settlement: resolve accumulated deltas to concrete values

This is a significant change to the MVHashMap data structure, similar in scope
to Aptos's `versioned_delayed_fields.rs`.

## Infrastructure Available

The following are committed and ready to use:
- `core/blockstm/balance_delta.go` — BalanceDeltaMap (can be repurposed)
- `core/mainnet_witness_benchmark_test.go` — comprehensive test harness:
  - TestMainnetConflictAnalysis: identifies conflict keys
  - TestMainnetDeltaFeasibility: proves balance ops are commutative  
  - TestMainnetSerialVsParallel: per-block timing comparison
  - TestMainnetOpcodeMetrics: VFails, suspensions, aborts per block
  - TestMainnetWitnessConsistency: correctness verification
  - ValidateVersionDiag: diagnostic validation with conflict key info

## Performance Numbers

| Approach | VFails | Speedup | Correct |
|---|---|---|---|
| Baseline (no changes) | ~100+/block | 0.8-1.1x | Yes |
| Skip addr+bal validation | 0-22/block | 1.35-2.23x | Yes* (unsafe for mining) |
| Separate BalanceDeltaMap | ~100+/block | 0.8-1.1x | No (8/9 wrong state) |
| MVHashMap FlagDelta (TODO) | ~0/block expected | ~1.7x expected | Expected correct |
