# Delta-Based Balance Reconciliation for BlockSTM

## Current Status

**Committed:** Address + balance subpath validation skip in `ValidateVersion`.
- Baseline parallel: 9/9 blocks correct, 1.35-2.17x speedup
- Opcode-level: 7/9 correct (2 blocks have suspension timing issues)
- Known limitation: `GetBalance` reads are not validated, which is unsafe for
  block mining if EVM logic branches on stale balance values

**FlagDelta MVHashMap infrastructure committed** but not wired into statedb:
- `FlagDelta` constant, `WriteCell.deltaAdd/deltaSub` fields
- `WriteDelta`, `ReadDelta`, `GetTxDelta` methods
- `DeltaReadResult` type
- `FlushMVWriteSet` balance key filtering
- Delta validation in `ValidateVersion` (TxnIndex == -2 marker)

## Next Step: Debug FlagDelta Settlement

The FlagDelta statedb wiring (AddBalance/SubBalance/GetBalance/settlement)
produces wrong state roots on 8/9 blocks. The settlement bug is in
`ApplyMVWriteSet` — the delta application via `GetTxDelta` either doesn't
find the entry or applies incorrect values.

**To debug:** Write a targeted test that traces one specific address's balance
through serial vs parallel settlement for the first 5 txs of block 0x4EC6D10.
Compare the base balance, accumulated deltas, and final balance at each step.

## Architecture

### MVHashMap Changes (committed)
- `FlagDelta = 2` alongside `FlagDone = 0`, `FlagEstimate = 1`
- `WriteCell` extended with `deltaAdd`, `deltaSub` (uint256.Int)
- `WriteDelta(k, v, add, sub)` creates/accumulates delta entries
- `ReadDelta(k, txIdx)` accumulates ALL deltas from tx 0..txIdx-1
- `GetTxDelta(k, txIdx)` retrieves a specific tx's delta
- `FlushMVWriteSet` skips balance subpath keys
- `ValidateVersion` handles TxnIndex == -2 delta reads via `ReadDelta`

### StateDB Changes (NOT committed — buggy settlement)
- `AddBalance`: calls `WriteDelta(balKey, version, amount, nil)` + local MVWrite
- `SubBalance`: calls `WriteDelta(balKey, version, nil, amount)` + local MVWrite
- `GetBalance`: uses `ReadDelta` to compute `trie_base + accumulated_deltas`
  - Self-write shortcut: if this tx already wrote balance, read from local stateObject
  - FlagEstimate: suspend (opcode-level) or abort (baseline)
  - Records ReadDescriptor with TxnIndex == -2 and delta version hash
- `mvRecordWritten` / `mvRecordWrittenStorageOnly`: fix stale balance on deep-copied
  stateObject using `ReadDelta` to compute `trie_base + accumulated_deltas`
- `ApplyMVWriteSet` (settlement): uses `GetTxDelta` to apply delta via
  `AddBalance`/`SubBalance` on finalStateDB instead of absolute SetBalance

### Key Correctness Properties
1. Balance operations are commutative (add/sub only) — mathematically proven
2. `ReadDelta` FlagEstimate handling ensures readers see consistent deltas
3. Delta validation catches stale delta snapshots
4. Settlement applies deltas in order → correct accumulated balance
