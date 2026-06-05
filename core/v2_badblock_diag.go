package core

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/blockstm"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

// V2 bad-block diagnostics. Enabled with BOR_V2_DIAG=1 (off by default).
// When V2's computed gas diverges from the canonical (header) gas, it freezes
// the failure: re-runs the block serially (the correct answer) to find the
// divergent tx, then diffs that tx's recorded V2 read-set against the
// serial-correct value for each key — naming the exact stale read. Output is a
// self-contained JSON bundle in BOR_V2_DIAG_DIR (default cwd).

func v2DiagEnabled() bool {
	v := os.Getenv("BOR_V2_DIAG")
	return v != "" && v != "0" && v != "false" && v != "off" && v != "no"
}

func v2DiagDir() string {
	if d := os.Getenv("BOR_V2_DIAG_DIR"); d != "" {
		return d
	}
	return "."
}

type v2DiagRead struct {
	Kind        string `json:"kind"` // nonce|code|state|create|suicide|unknown
	Addr        string `json:"addr"`
	Slot        string `json:"slot,omitempty"`
	KeyHex      string `json:"key_hex"`
	V2WriterIdx int    `json:"v2_writer_idx"`
	V2WriterInc int    `json:"v2_writer_inc"`
	V2Value     string `json:"v2_value"`
	SerialValue string `json:"serial_value"`
	Stale       bool   `json:"stale"`
}

type v2DiagTx struct {
	Index     int    `json:"index"`
	Hash      string `json:"hash"`
	Type      uint8  `json:"type"`
	V2Gas     uint64 `json:"v2_gas"`
	SerialGas uint64 `json:"serial_gas"`
	SerialErr string `json:"serial_err,omitempty"`
	Divergent bool   `json:"divergent"`
}

type v2DiagBundle struct {
	Time           string                  `json:"time"`
	Number         uint64                  `json:"number"`
	Hash           string                  `json:"hash"`
	ParentHash     string                  `json:"parent_hash"`
	ParentRoot     string                  `json:"parent_root"`
	HeaderGasUsed  uint64                  `json:"header_gas_used"`
	V2GasUsed      uint64                  `json:"v2_gas_used"`
	ExecCount      int                     `json:"v2_exec_count"`
	VFailCount     int                     `json:"v2_vfail_count"`
	VFailCats      map[string]int          `json:"v2_vfail_cats"`
	DivergentTxs   []int                   `json:"divergent_txs"`
	PerTx          []v2DiagTx              `json:"per_tx"`
	DivergentReads map[string][]v2DiagRead `json:"divergent_reads"` // tx index -> reads (stale flagged)
	BlockRLP       string                  `json:"block_rlp"`
}

func fmtDiagVal(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case uint64:
		return fmt.Sprintf("%d", x)
	case []byte:
		return hexutil.Encode(x)
	case common.Hash:
		return x.Hex()
	case bool:
		return fmt.Sprintf("%t", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// readSerialKey decodes a blockstm key and reads its current value from the
// (serial, correct) state — the value V2 should have read.
func readSerialKey(s *state.StateDB, k blockstm.Key) (kind, slot string, val interface{}) {
	addr := k.GetAddress()
	if k.IsState() {
		sl := k.GetStateKey()
		return "state", sl.Hex(), s.GetState(addr, sl)
	}
	if k.IsSubpath() {
		switch k.GetSubpath() {
		case state.NoncePath:
			return "nonce", "", s.GetNonce(addr)
		case state.CodePath:
			return "code", "", s.GetCode(addr)
		case state.CreatePath:
			return "create", "", s.Exist(addr)
		case state.SuicidePath:
			// V2-only tracking; serial has no direct equivalent.
			return "suicide", "", nil
		}
		return "subpath", "", nil
	}
	return "unknown", "", nil
}

// dumpV2BadBlock writes the diagnostic bundle. diagBase must be a clean copy of
// the post-system-call parent state (pre-settlement), captured before execution.
func dumpV2BadBlock(block *types.Block, parentRoot common.Hash, blockCtx vm.BlockContext,
	config *params.ChainConfig, result *V2ExecutionResult, diagBase *state.StateDB, baseFee *big.Int) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("V2 diag dump panicked (ignored)", "err", r)
		}
	}()

	txs := block.Transactions()
	signer := types.MakeSigner(config, block.Number(), block.Time())

	serialGas := make([]uint64, len(txs))
	serialErr := make([]string, len(txs))

	// runSerial executes txs [0,upTo) on base, recording per-tx gas. base is
	// mutated; callers pass a fresh Copy().
	runSerial := func(upTo int, base *state.StateDB) {
		gp := new(GasPool).AddGas(block.GasLimit())
		evm := vm.NewEVM(blockCtx, base, config, vm.Config{})
		for i := 0; i < upTo; i++ {
			// Bor's synthetic state-sync tx isn't a V2 task and isn't applied
			// like a normal tx; skip it so it doesn't show as a false divergence.
			if txs[i].Type() == types.StateSyncTxType {
				continue
			}
			msg, err := TransactionToMessage(txs[i], signer, baseFee)
			if err != nil {
				serialErr[i] = err.Error()
				continue
			}
			base.SetTxContext(txs[i].Hash(), i)
			evm.SetTxContext(NewEVMTxContext(msg))
			res, err := ApplyMessage(evm, msg, gp)
			if err != nil {
				serialErr[i] = err.Error()
				continue
			}
			if res != nil {
				serialGas[i] = res.UsedGas
			}
			base.Finalise(true)
		}
	}
	runSerial(len(txs), diagBase.Copy())

	bundle := &v2DiagBundle{
		Time:           time.Now().UTC().Format(time.RFC3339),
		Number:         block.NumberU64(),
		Hash:           block.Hash().Hex(),
		ParentHash:     block.ParentHash().Hex(),
		ParentRoot:     parentRoot.Hex(),
		HeaderGasUsed:  block.GasUsed(),
		V2GasUsed:      result.GasUsed,
		ExecCount:      result.ExecCount,
		VFailCount:     result.VFailCount,
		VFailCats:      result.VFailCats,
		DivergentReads: map[string][]v2DiagRead{},
	}

	var divergent []int
	for i, tx := range txs {
		var v2g uint64
		if i < len(result.Pdbs) && result.Pdbs[i] != nil {
			v2g = result.Pdbs[i].UsedGas
		}
		d := v2g != serialGas[i]
		if d {
			divergent = append(divergent, i)
		}
		bundle.PerTx = append(bundle.PerTx, v2DiagTx{
			Index: i, Hash: tx.Hash().Hex(), Type: tx.Type(),
			V2Gas: v2g, SerialGas: serialGas[i], SerialErr: serialErr[i], Divergent: d,
		})
	}
	bundle.DivergentTxs = divergent

	// For each divergent tx, diff its recorded V2 read-set against the
	// serial-correct value at that tx's execution point.
	for _, idx := range divergent {
		if idx >= len(result.Pdbs) || result.Pdbs[idx] == nil {
			continue
		}
		base := diagBase.Copy()
		runSerial(idx, base) // serial state immediately before tx idx
		reads := make([]v2DiagRead, 0, len(result.Pdbs[idx].StoreReads))
		for i := range result.Pdbs[idx].StoreReads {
			rd := &result.Pdbs[idx].StoreReads[i]
			r := v2DiagRead{
				Addr:        rd.Key.GetAddress().Hex(),
				KeyHex:      hexutil.Encode(rd.Key[:]),
				V2WriterIdx: rd.WriterIdx,
				V2WriterInc: rd.WriterInc,
				V2Value:     fmtDiagVal(rd.StoreVal),
			}
			var sv interface{}
			r.Kind, r.Slot, sv = readSerialKey(base, rd.Key)
			r.SerialValue = fmtDiagVal(sv)
			r.Stale = r.Kind != "suicide" && r.V2Value != r.SerialValue
			reads = append(reads, r)
		}
		bundle.DivergentReads[fmt.Sprintf("%d", idx)] = reads
	}

	if rlpBytes, err := rlp.EncodeToBytes(block); err == nil {
		bundle.BlockRLP = hexutil.Encode(rlpBytes)
	}

	if err := os.MkdirAll(v2DiagDir(), 0o755); err != nil {
		log.Error("V2 diag mkdir failed", "err", err)
		return
	}
	fname := filepath.Join(v2DiagDir(),
		fmt.Sprintf("v2_badblock_%d_%s.json", block.NumberU64(), block.Hash().Hex()[2:12]))
	data, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		log.Error("V2 diag marshal failed", "err", err)
		return
	}
	if err := os.WriteFile(fname, data, 0o644); err != nil {
		log.Error("V2 diag write failed", "err", err)
		return
	}
	log.Error("V2 BAD BLOCK diagnostic written", "file", fname,
		"number", block.NumberU64(), "divergentTxs", divergent,
		"v2Gas", result.GasUsed, "headerGas", block.GasUsed())
}
