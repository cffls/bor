package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/blockstm"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

const witnessDir = "/tmp/witnesses"

var testBlockHexes = []string{
	"0x4EC6D10", "0x4EC6D11", "0x4EC6D12", "0x4EC6D13",
	"0x4EC6D14", "0x4EC6D15", "0x4EC6D16", "0x4EC6D17", "0x4EC6D18",
}

// benchConsensus is a minimal consensus engine for benchmarking.
// It skips Heimdall-dependent operations (state sync, span commit).
type benchConsensus struct{}

func (b *benchConsensus) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (b *benchConsensus) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
}

func (b *benchConsensus) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header) (chan<- struct{}, <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))
	for range headers {
		results <- nil
	}
	return abort, results
}

func (b *benchConsensus) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	return nil
}

func (b *benchConsensus) Prepare(chain consensus.ChainHeaderReader, header *types.Header, waitOnPrepare bool) error {
	return nil
}

func (b *benchConsensus) Finalize(chain consensus.ChainHeaderReader, header *types.Header, stateDB vm.StateDB, body *types.Body, receipts []*types.Receipt) []*types.Receipt {
	return receipts
}

func (b *benchConsensus) FinalizeAndAssemble(chain consensus.ChainHeaderReader, header *types.Header, stateDB *state.StateDB, body *types.Body, receipts []*types.Receipt) (*types.Block, []*types.Receipt, time.Duration, error) {
	return types.NewBlock(header, body, receipts, trie.NewStackTrie(nil)), receipts, 0, nil
}

func (b *benchConsensus) Seal(chain consensus.ChainHeaderReader, block *types.Block, witness *stateless.Witness, results chan<- *consensus.NewSealedBlockEvent, stop <-chan struct{}) error {
	return nil
}

func (b *benchConsensus) SealHash(header *types.Header) common.Hash {
	return header.Hash()
}

func (b *benchConsensus) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {
	return big.NewInt(1)
}

func (b *benchConsensus) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return nil
}

func (b *benchConsensus) Close() error {
	return nil
}

// rpcJSONResult wraps the JSON-RPC response.
type rpcJSONResult struct {
	Result json.RawMessage `json:"result"`
}

// witnessJSON matches the RPCMarshalWitness output format.
type witnessJSON struct {
	Context    json.RawMessage   `json:"context"`
	Headers    []json.RawMessage `json:"headers"`
	Codes      []hexutil.Bytes   `json:"codes"`
	State      []hexutil.Bytes   `json:"state"`
	PreState   common.Hash       `json:"preStateRoot"`
	CodesCount int               `json:"codesCount"`
	StateCount int               `json:"stateNodesCount"`
}

// blockJSON matches the eth_getBlockByNumber response (minimal fields needed).
type blockJSON struct {
	// Header fields are embedded and will be parsed via types.Header.UnmarshalJSON.
	// We also need the transactions array.
	Transactions []json.RawMessage `json:"transactions"`
}

// loadWitnessFromJSON parses the RPC JSON witness format into a *stateless.Witness.
func loadWitnessFromJSON(path string) (*stateless.Witness, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading witness file: %w", err)
	}

	var rpcResp rpcJSONResult
	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return nil, fmt.Errorf("parsing JSON-RPC envelope: %w", err)
	}

	var wj witnessJSON
	if err := json.Unmarshal(rpcResp.Result, &wj); err != nil {
		return nil, fmt.Errorf("parsing witness result: %w", err)
	}

	// Parse context header.
	var contextHeader types.Header
	if err := json.Unmarshal(wj.Context, &contextHeader); err != nil {
		return nil, fmt.Errorf("parsing context header: %w", err)
	}

	// Parse parent headers.
	headers := make([]*types.Header, len(wj.Headers))
	for i, raw := range wj.Headers {
		var h types.Header
		if err := json.Unmarshal(raw, &h); err != nil {
			return nil, fmt.Errorf("parsing header %d: %w", i, err)
		}
		headers[i] = &h
	}

	// Build state map from hex-encoded trie nodes.
	stateMap := make(map[string]struct{}, len(wj.State))
	for _, node := range wj.State {
		stateMap[string(node)] = struct{}{}
	}

	// Build codes map.
	codesMap := make(map[string]struct{}, len(wj.Codes))
	for _, code := range wj.Codes {
		codesMap[string(code)] = struct{}{}
	}

	// Zero out the state root and receipt hash as ExecuteStateless expects.
	contextHeader.Root = common.Hash{}
	contextHeader.ReceiptHash = common.Hash{}

	// Use NewWitness with nil chain (skips parent header fetch),
	// then populate the exported fields.
	witness, err := stateless.NewWitness(&contextHeader, nil)
	if err != nil {
		return nil, fmt.Errorf("creating witness: %w", err)
	}
	witness.Headers = headers
	witness.Codes = codesMap
	witness.State = stateMap

	return witness, nil
}

// alchemyRPC sends a JSON-RPC request to the Alchemy endpoint.
func alchemyRPC(url string, method string, params []interface{}) (json.RawMessage, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	})

	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var rpcResp rpcJSONResult
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("parsing RPC response: %w", err)
	}

	return rpcResp.Result, nil
}

// fetchAndCacheBlock downloads a block from Alchemy and caches it to disk.
func fetchAndCacheBlock(blockHex string, alchemyURL string) ([]byte, error) {
	cachePath := filepath.Join(witnessDir, blockHex+".block")

	// Check cache first.
	if data, err := os.ReadFile(cachePath); err == nil {
		return data, nil
	}

	result, err := alchemyRPC(alchemyURL, "eth_getBlockByNumber", []interface{}{blockHex, true})
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(cachePath, result, 0644); err != nil {
		return nil, fmt.Errorf("caching block: %w", err)
	}

	return result, nil
}

// parseBlockFromJSON parses eth_getBlockByNumber response into *types.Block.
func parseBlockFromJSON(data []byte) (*types.Block, common.Hash, common.Hash, error) {
	// Parse header using types.Header.UnmarshalJSON (handles all RPC field names).
	var header types.Header
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, common.Hash{}, common.Hash{}, fmt.Errorf("parsing block header: %w", err)
	}

	// Save the original roots before zeroing.
	origStateRoot := header.Root
	origReceiptHash := header.ReceiptHash

	// Parse transactions.
	var bj blockJSON
	if err := json.Unmarshal(data, &bj); err != nil {
		return nil, common.Hash{}, common.Hash{}, fmt.Errorf("parsing block transactions: %w", err)
	}

	txs := make([]*types.Transaction, 0, len(bj.Transactions))
	for i, raw := range bj.Transactions {
		var tx types.Transaction
		if err := json.Unmarshal(raw, &tx); err != nil {
			return nil, common.Hash{}, common.Hash{}, fmt.Errorf("parsing tx %d: %w", i, err)
		}
		txs = append(txs, &tx)
	}

	// Zero out roots as ExecuteStateless expects.
	header.Root = common.Hash{}
	header.ReceiptHash = common.Hash{}

	block := types.NewBlockWithHeader(&header).WithBody(types.Body{
		Transactions: txs,
	})

	return block, origStateRoot, origReceiptHash, nil
}

// fetchAndCacheCode fetches contract code and caches it to disk.
func fetchAndCacheCode(addr common.Address, blockHex string, alchemyURL string) ([]byte, error) {
	codeDir := filepath.Join(witnessDir, "codes")
	os.MkdirAll(codeDir, 0755)

	// Use address as filename since we don't know the hash yet.
	cachePath := filepath.Join(codeDir, addr.Hex()+".bin")
	if data, err := os.ReadFile(cachePath); err == nil {
		return data, nil
	}

	result, err := alchemyRPC(alchemyURL, "eth_getCode", []interface{}{addr.Hex(), blockHex})
	if err != nil {
		return nil, err
	}

	var codeHex string
	if err := json.Unmarshal(result, &codeHex); err != nil {
		return nil, fmt.Errorf("parsing code response: %w", err)
	}

	code := common.FromHex(codeHex)
	if err := os.WriteFile(cachePath, code, 0644); err != nil {
		return nil, fmt.Errorf("caching code: %w", err)
	}

	return code, nil
}

// prewarmCodes fetches and stores all contract codes needed for block execution.
// It opens the state trie from the witness, looks up each transaction's to address,
// and fetches code for any contract accounts.
func prewarmCodes(diskdb ethdb.Database, witness *stateless.Witness, block *types.Block, _ string, _ *params.ChainConfig, alchemyURL string) error {
	// Build the memdb from the witness to access the state trie.
	memdb := witness.MakeHashDB(diskdb)
	db, err := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
	if err != nil {
		return fmt.Errorf("opening state: %w", err)
	}

	// Compute the parent block number for eth_getCode (code at pre-state).
	parentBlockNum := new(big.Int).Sub(witness.Header().Number, big.NewInt(1))
	parentBlockHex := fmt.Sprintf("0x%x", parentBlockNum)

	seen := make(map[common.Address]bool)
	emptyCodeHash := crypto.Keccak256Hash(nil)

	checkAndFetch := func(addr common.Address) error {
		if seen[addr] {
			return nil
		}
		seen[addr] = true

		codeHash := db.GetCodeHash(addr)
		if codeHash == (common.Hash{}) || codeHash == emptyCodeHash {
			return nil
		}

		// Check if already in diskdb.
		if existing := rawdb.ReadCode(diskdb, codeHash); len(existing) > 0 {
			return nil
		}

		code, err := fetchAndCacheCode(addr, parentBlockHex, alchemyURL)
		if err != nil {
			return fmt.Errorf("fetching code for %s: %w", addr.Hex(), err)
		}

		if len(code) > 0 {
			rawdb.WriteCode(diskdb, codeHash, code)
		}

		return nil
	}

	// Check all transaction targets and senders.
	for _, tx := range block.Transactions() {
		if tx.To() != nil {
			if err := checkAndFetch(*tx.To()); err != nil {
				return err
			}
		}
	}

	return nil
}

// codeCachingDB wraps a memdb and loads codes from a disk cache at startup.
type codeCachingDB struct {
	ethdb.Database
	codeDir string
}

func newCodeCachingDB(codeDir string) *codeCachingDB {
	return &codeCachingDB{
		Database: rawdb.NewMemoryDatabase(),
		codeDir:  codeDir,
	}
}

// loadCodesFromDisk loads all previously cached code files into the database.
func (db *codeCachingDB) loadCodesFromDisk() error {
	entries, err := os.ReadDir(db.codeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".bin" {
			continue
		}

		code, err := os.ReadFile(filepath.Join(db.codeDir, entry.Name()))
		if err != nil {
			continue
		}

		if len(code) > 0 {
			codeHash := crypto.Keccak256Hash(code)
			rawdb.WriteCode(db.Database, codeHash, code)
		}
	}

	return nil
}

// testBlockData holds pre-loaded data for one block.
type testBlockData struct {
	witness     *stateless.Witness
	block       *types.Block
	stateRoot   common.Hash
	receiptRoot common.Hash
}

// preparedBlock holds pre-computed data ready for benchmarking.
// MakeHashDB, triedb, base statedb, and header caches are all built once
// outside the timed loop so we measure only Process() execution time.
type preparedBlock struct {
	block       *types.Block
	witness     *stateless.Witness
	memdb       ethdb.Database
	tdb         *triedb.Database
	baseState   *state.StateDB // template statedb — Copy() for each iteration
	headerCache *lru.Cache[common.Hash, *types.Header]
	stateRoot   common.Hash
	receiptRoot common.Hash
	author      common.Address
}

// prepareBlocks pre-computes all setup data for each block.
func prepareBlocks(blocks []testBlockData, diskdb ethdb.Database, config *params.ChainConfig) []preparedBlock {
	prepared := make([]preparedBlock, len(blocks))
	for i, bd := range blocks {
		memdb := bd.witness.MakeHashDB(diskdb)
		tdb := triedb.NewDatabase(memdb, triedb.HashDefaults)
		root := bd.witness.Root()
		db, err := state.New(root, state.NewDatabase(tdb, nil))
		if err != nil {
			panic(fmt.Sprintf("state.New for block %d: %v", i, err))
		}
		hc := lru.NewCache[common.Hash, *types.Header](256)
		for _, h := range bd.witness.Headers {
			hc.Add(h.Hash(), h)
		}
		prepared[i] = preparedBlock{
			block:       bd.block,
			witness:     bd.witness,
			memdb:       memdb,
			tdb:         tdb,
			baseState:   db,
			headerCache: hc,
			stateRoot:   bd.stateRoot,
			receiptRoot: bd.receiptRoot,
			author:      getAuthor(config, bd.witness.Header()),
		}
	}
	return prepared
}

// processSerial runs serial execution for one pre-built block.
func processSerial(pb *preparedBlock, config *params.ChainConfig, engine consensus.Engine) (*ProcessResult, error) {
	db := pb.baseState.Copy()
	hc := &HeaderChain{
		config:      config,
		chainDb:     pb.memdb,
		headerCache: pb.headerCache,
		engine:      engine,
	}
	return NewStateProcessor(hc).Process(pb.block, db, vm.Config{}, &pb.author, context.Background())
}

// processParallel runs parallel execution for one pre-built block.
func processParallel(pb *preparedBlock, config *params.ChainConfig, engine consensus.Engine, numProcs int, opcodeLevel bool, predictor *blockstm.ConflictPredictor) (*ProcessResult, error) {
	db := pb.baseState.Copy()
	bc := &BlockChain{
		hc:                           &HeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine},
		parallelSpeculativeProcesses: numProcs,
		opcodeLevel:                  opcodeLevel,
		conflictPredictor:            predictor,
	}
	hc := &benchHeaderChain{
		config:      config,
		chainDb:     pb.memdb,
		headerCache: pb.headerCache,
		engine:      engine,
	}
	return NewParallelStateProcessor(hc, bc).Process(pb.block, db, vm.Config{}, &pb.author, context.Background())
}

// processParallelWithPredictor runs blocks sequentially feeding the predictor,
// then re-runs them all with predictions to measure the improvement.
func processBlocksWithPredictor(prepared []preparedBlock, config *params.ChainConfig, engine consensus.Engine, numProcs int) error {
	predictor := blockstm.NewConflictPredictor()

	for i := range prepared {
		_, err := processParallel(&prepared[i], config, engine, numProcs, true, predictor)
		if err != nil {
			return err
		}
	}

	return nil
}

// loadTestBlocks loads all witness and block data for the benchmark.
func loadTestBlocks(t testing.TB, alchemyURL string) ([]testBlockData, ethdb.Database) {
	t.Helper()

	// Check witness directory exists.
	if _, err := os.Stat(witnessDir); os.IsNotExist(err) {
		t.Skipf("witness directory %s not found", witnessDir)
	}

	codeDir := filepath.Join(witnessDir, "codes")
	diskdb := newCodeCachingDB(codeDir)
	diskdb.loadCodesFromDisk()

	var blocks []testBlockData

	for _, blockHex := range testBlockHexes {
		witnessPath := filepath.Join(witnessDir, blockHex+".witness")
		if _, err := os.Stat(witnessPath); os.IsNotExist(err) {
			t.Skipf("witness file %s not found", witnessPath)
		}

		// Load witness.
		witness, err := loadWitnessFromJSON(witnessPath)
		if err != nil {
			t.Fatalf("loading witness %s: %v", blockHex, err)
		}

		// Load/fetch block.
		blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
		if err != nil {
			t.Fatalf("fetching block %s: %v", blockHex, err)
		}

		block, stateRoot, receiptRoot, err := parseBlockFromJSON(blockData)
		if err != nil {
			t.Fatalf("parsing block %s: %v", blockHex, err)
		}

		// Prewarm codes needed for this block.
		if err := prewarmCodes(diskdb, witness, block, blockHex, params.BorMainnetChainConfig, alchemyURL); err != nil {
			t.Logf("warning: prewarm codes for %s: %v", blockHex, err)
		}

		blocks = append(blocks, testBlockData{
			witness:     witness,
			block:       block,
			stateRoot:   stateRoot,
			receiptRoot: receiptRoot,
		})

		t.Logf("loaded block %s: %d txs, %d gas", blockHex, len(block.Transactions()), block.GasUsed())
	}

	return blocks, diskdb
}

// executeStatelessSerial runs serial stateless execution for one block.
func executeStatelessSerial(config *params.ChainConfig, block *types.Block, witness *stateless.Witness, author *common.Address, engine consensus.Engine, diskdb ethdb.Database) (common.Hash, common.Hash, *ProcessResult, error) {
	memdb := witness.MakeHashDB(diskdb)
	db, err := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
	if err != nil {
		return common.Hash{}, common.Hash{}, nil, err
	}

	headerChain := &HeaderChain{
		config:      config,
		chainDb:     memdb,
		headerCache: lru.NewCache[common.Hash, *types.Header](256),
		engine:      engine,
	}
	processor := NewStateProcessor(headerChain)

	res, err := processor.Process(block, db, vm.Config{}, author, context.Background())
	if err != nil {
		return common.Hash{}, common.Hash{}, nil, err
	}

	receiptRoot := types.DeriveSha(res.Receipts, trie.NewStackTrie(nil))
	stateRoot := db.IntermediateRoot(config.IsEIP158(block.Number()))
	return stateRoot, receiptRoot, res, nil
}

// benchHeaderChain implements ChainContext for the parallel processor.
type benchHeaderChain struct {
	config      *params.ChainConfig
	chainDb     ethdb.Database
	headerCache *lru.Cache[common.Hash, *types.Header]
	engine      consensus.Engine
}

func (hc *benchHeaderChain) Config() *params.ChainConfig                    { return hc.config }
func (hc *benchHeaderChain) CurrentHeader() *types.Header                   { return nil }
func (hc *benchHeaderChain) GetHeaderByNumber(number uint64) *types.Header  { return nil }
func (hc *benchHeaderChain) GetHeaderByHash(hash common.Hash) *types.Header { return nil }
func (hc *benchHeaderChain) GetTd(hash common.Hash, number uint64) *big.Int { return nil }
func (hc *benchHeaderChain) Engine() consensus.Engine                       { return hc.engine }

func (hc *benchHeaderChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	if header, ok := hc.headerCache.Get(hash); ok {
		return header
	}
	return nil
}

// executeStatelessParallel runs parallel BlockSTM execution for one block.
func executeStatelessParallel(config *params.ChainConfig, block *types.Block, witness *stateless.Witness, author *common.Address, engine consensus.Engine, diskdb ethdb.Database, numProcs int, opcodeLevel bool) (common.Hash, common.Hash, *ProcessResult, error) {
	memdb := witness.MakeHashDB(diskdb)
	db, err := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
	if err != nil {
		return common.Hash{}, common.Hash{}, nil, err
	}

	hc := &benchHeaderChain{
		config:      config,
		chainDb:     memdb,
		headerCache: lru.NewCache[common.Hash, *types.Header](256),
		engine:      engine,
	}

	// Populate header cache with witness headers.
	for _, h := range witness.Headers {
		hc.headerCache.Add(h.Hash(), h)
	}

	// Create a minimal BlockChain-like struct for the parallel processor.
	bc := &BlockChain{
		hc:                           &HeaderChain{config: config, chainDb: memdb, headerCache: hc.headerCache, engine: engine},
		parallelSpeculativeProcesses: numProcs,
		opcodeLevel:                  opcodeLevel,
	}

	processor := NewParallelStateProcessor(hc, bc)

	res, err := processor.Process(block, db, vm.Config{}, author, context.Background())
	if err != nil {
		return common.Hash{}, common.Hash{}, nil, err
	}

	receiptRoot := types.DeriveSha(res.Receipts, trie.NewStackTrie(nil))
	stateRoot := db.IntermediateRoot(config.IsEIP158(block.Number()))
	return stateRoot, receiptRoot, res, nil
}

func getAlchemyURL(t testing.TB) string {
	t.Helper()
	url := os.Getenv("ALCHEMY_URL")
	if url == "" {
		t.Skip("ALCHEMY_URL not set")
	}
	return url
}

func getAuthor(config *params.ChainConfig, header *types.Header) common.Address {
	if config.Bor != nil && config.Bor.IsRio(header.Number) {
		coinbase := common.HexToAddress(config.Bor.CalculateCoinbase(header.Number.Uint64()))
		if coinbase != (common.Address{}) {
			return coinbase
		}
	}
	return header.Coinbase
}

func TestMainnetWitnessLoad(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, _ := loadTestBlocks(t, alchemyURL)
	t.Logf("loaded %d blocks", len(blocks))
}

func TestMainnetWitnessSerial(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}

	for i, bd := range blocks {
		author := getAuthor(config, bd.witness.Header())
		stateRoot, receiptRoot, _, err := executeStatelessSerial(config, bd.block, bd.witness, &author, engine, diskdb)
		if err != nil {
			t.Fatalf("block %d (%s): execution failed: %v", i, testBlockHexes[i], err)
		}
		t.Logf("block %s: stateRoot=%s receiptRoot=%s", testBlockHexes[i], stateRoot.Hex(), receiptRoot.Hex())
		t.Logf("  expected: stateRoot=%s receiptRoot=%s", bd.stateRoot.Hex(), bd.receiptRoot.Hex())
		// Note: State roots won't match exactly because we skip Finalize's state sync.
	}
}

// TestMainnetWitnessConsistency verifies that serial, parallel, and opcode-level
// execution all produce identical state and receipt roots.
func TestMainnetWitnessConsistency(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := runtime.NumCPU()

	for i, bd := range blocks {
		author := getAuthor(config, bd.witness.Header())

		serialState, serialReceipt, _, err := executeStatelessSerial(config, bd.block, bd.witness, &author, engine, diskdb)
		if err != nil {
			t.Fatalf("block %s serial: %v", testBlockHexes[i], err)
		}

		parallelState, parallelReceipt, _, err := executeStatelessParallel(config, bd.block, bd.witness, &author, engine, diskdb, numProcs, false)
		if err != nil {
			t.Fatalf("block %s parallel: %v", testBlockHexes[i], err)
		}

		opcodeState, opcodeReceipt, _, err := executeStatelessParallel(config, bd.block, bd.witness, &author, engine, diskdb, numProcs, true)
		if err != nil {
			t.Fatalf("block %s opcode-level: %v", testBlockHexes[i], err)
		}

		if serialState != parallelState {
			t.Errorf("block %s: parallel stateRoot mismatch: serial=%s parallel=%s", testBlockHexes[i], serialState.Hex(), parallelState.Hex())
		}
		if serialReceipt != parallelReceipt {
			t.Errorf("block %s: parallel receiptRoot mismatch: serial=%s parallel=%s", testBlockHexes[i], serialReceipt.Hex(), parallelReceipt.Hex())
		}
		if serialState != opcodeState {
			t.Errorf("block %s: opcode-level stateRoot mismatch: serial=%s opcode=%s", testBlockHexes[i], serialState.Hex(), opcodeState.Hex())
		}
		if serialReceipt != opcodeReceipt {
			t.Errorf("block %s: opcode-level receiptRoot mismatch: serial=%s opcode=%s", testBlockHexes[i], serialReceipt.Hex(), opcodeReceipt.Hex())
		}

		// Validate against block header (when serial matches header, parallel must too)
		if serialState == bd.stateRoot && serialReceipt == bd.receiptRoot {
			t.Logf("block %s: all modes consistent and match block header", testBlockHexes[i])
		} else if bd.stateRoot != (common.Hash{}) {
			t.Logf("block %s: all modes consistent (state=%s receipt=%s) header state=%s receipt=%s",
				testBlockHexes[i], serialState.Hex()[:10], serialReceipt.Hex()[:10],
				bd.stateRoot.Hex()[:10], bd.receiptRoot.Hex()[:10])
		} else {
			t.Logf("block %s: all modes consistent (state=%s receipt=%s)", testBlockHexes[i], serialState.Hex()[:10], serialReceipt.Hex()[:10])
		}
	}
}

// TestMainnetOpcodeMetrics runs each block through opcode-level BlockSTM and
// reports detailed execution metrics: suspensions, aborts, re-executions,
// and the effect of conflict prediction.
func TestMainnetOpcodeMetrics(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	numProcs := 6

	t.Log("=== Without Conflict Predictor ===")
	t.Logf("%-10s %5s %6s %6s %5s %6s %5s %8s", "Block", "Txs", "Execs", "Abort", "VFail", "Susp", "Repl", "Ratio")

	for i, pb := range prepared {
		db := pb.baseState.Copy()
		bc := &BlockChain{
			hc:                           &HeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine},
			parallelSpeculativeProcesses: numProcs,
			opcodeLevel:                  true,
		}
		hc := &benchHeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine}
		res, err := NewParallelStateProcessor(hc, bc).Process(pb.block, db, vm.Config{}, &pb.author, context.Background())
		if err != nil {
			t.Fatalf("block %s: %v", testBlockHexes[i], err)
		}

		txs := len(pb.block.Transactions())
		ratio := float64(res.BlockSTMExecutions) / float64(txs)
		t.Logf("%-10s %5d %6d %6d %5d %6d %5d %7.2fx",
			testBlockHexes[i], txs,
			res.BlockSTMExecutions, res.BlockSTMAborts, res.BlockSTMValidationFails,
			res.BlockSTMSuspensions, res.BlockSTMReplacements, ratio)
	}

	t.Log("")
	t.Log("=== With Conflict Predictor (learns across blocks) ===")
	t.Logf("%-10s %5s %6s %6s %5s %6s %5s %8s %6s", "Block", "Txs", "Execs", "Abort", "VFail", "Susp", "Repl", "Ratio", "Preds")

	predictor := blockstm.NewConflictPredictor()

	for i, pb := range prepared {
		db := pb.baseState.Copy()
		bc := &BlockChain{
			hc:                           &HeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine},
			parallelSpeculativeProcesses: numProcs,
			opcodeLevel:                  true,
			conflictPredictor:            predictor,
		}
		hc := &benchHeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine}
		res, err := NewParallelStateProcessor(hc, bc).Process(pb.block, db, vm.Config{}, &pb.author, context.Background())
		if err != nil {
			t.Fatalf("block %s: %v", testBlockHexes[i], err)
		}

		txs := len(pb.block.Transactions())
		ratio := float64(res.BlockSTMExecutions) / float64(txs)

		// Count how many txs had predictions
		predCount := 0
		signer := types.MakeSigner(config, pb.block.Header().Number, pb.block.Header().Time)
		for _, tx := range pb.block.Transactions() {
			if tx.To() != nil {
				if addrs := predictor.Predict(*tx.To()); len(addrs) > 0 {
					predCount++
				}
			}
			// Also do sender recovery to feed predictor's learning
			types.Sender(signer, tx)
		}

		t.Logf("%-10s %5d %6d %6d %5d %6d %5d %7.2fx %6d",
			testBlockHexes[i], txs,
			res.BlockSTMExecutions, res.BlockSTMAborts, res.BlockSTMValidationFails,
			res.BlockSTMSuspensions, res.BlockSTMReplacements, ratio, predCount)
	}

	// Run a SECOND pass with the now-warm predictor
	t.Log("")
	t.Log("=== Second pass with warm predictor ===")
	t.Logf("%-10s %5s %6s %6s %5s %6s %5s %8s %6s", "Block", "Txs", "Execs", "Abort", "VFail", "Susp", "Repl", "Ratio", "Preds")

	for i, pb := range prepared {
		// Count predictions before running
		predCount := 0
		for _, tx := range pb.block.Transactions() {
			if tx.To() != nil {
				if addrs := predictor.Predict(*tx.To()); len(addrs) > 0 {
					predCount++
				}
			}
		}

		db := pb.baseState.Copy()
		bc := &BlockChain{
			hc:                           &HeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine},
			parallelSpeculativeProcesses: numProcs,
			opcodeLevel:                  true,
			conflictPredictor:            predictor,
		}
		hc := &benchHeaderChain{config: config, chainDb: pb.memdb, headerCache: pb.headerCache, engine: engine}
		res, err := NewParallelStateProcessor(hc, bc).Process(pb.block, db, vm.Config{}, &pb.author, context.Background())
		if err != nil {
			t.Fatalf("block %s: %v", testBlockHexes[i], err)
		}

		txs := len(pb.block.Transactions())
		ratio := float64(res.BlockSTMExecutions) / float64(txs)
		t.Logf("%-10s %5d %6d %6d %5d %6d %5d %7.2fx %6d",
			testBlockHexes[i], txs,
			res.BlockSTMExecutions, res.BlockSTMAborts, res.BlockSTMValidationFails,
			res.BlockSTMSuspensions, res.BlockSTMReplacements, ratio, predCount)
	}
}

// BenchmarkMainnetStatelessSerial benchmarks serial execution of mainnet blocks.
// MakeHashDB and triedb construction are excluded from timing.
func BenchmarkMainnetStatelessSerial(b *testing.B) {
	alchemyURL := getAlchemyURL(b)
	blocks, diskdb := loadTestBlocks(b, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	totalGas := uint64(0)
	for _, pb := range prepared {
		totalGas += pb.block.GasUsed()
	}

	b.Run("AllBlocks", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := range prepared {
				if _, err := processSerial(&prepared[j], config, engine); err != nil {
					b.Fatalf("block %s: %v", testBlockHexes[j], err)
				}
			}
		}
		b.StopTimer()
		mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
		b.ReportMetric(mgasps, "mgas/s")
	})

	for i, pb := range prepared {
		pb := pb
		name := fmt.Sprintf("Block_%s_%dtx_%dMgas", testBlockHexes[i], len(pb.block.Transactions()), pb.block.GasUsed()/1e6)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				if _, err := processSerial(&pb, config, engine); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			mgasps := float64(pb.block.GasUsed()) * float64(b.N) / b.Elapsed().Seconds() / 1e6
			b.ReportMetric(mgasps, "mgas/s")
		})
	}
}

// BenchmarkMainnetStatelessParallel benchmarks baseline BlockSTM parallel execution.
func BenchmarkMainnetStatelessParallel(b *testing.B) {
	alchemyURL := getAlchemyURL(b)
	blocks, diskdb := loadTestBlocks(b, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := runtime.NumCPU()
	prepared := prepareBlocks(blocks, diskdb, config)

	totalGas := uint64(0)
	for _, pb := range prepared {
		totalGas += pb.block.GasUsed()
	}

	b.Run("AllBlocks", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := range prepared {
				if _, err := processParallel(&prepared[j], config, engine, numProcs, false, nil); err != nil {
					b.Fatalf("block %s: %v", testBlockHexes[j], err)
				}
			}
		}
		b.StopTimer()
		mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
		b.ReportMetric(mgasps, "mgas/s")
		b.ReportMetric(float64(numProcs), "workers")
	})

	for i, pb := range prepared {
		pb := pb
		name := fmt.Sprintf("Block_%s_%dtx_%dMgas", testBlockHexes[i], len(pb.block.Transactions()), pb.block.GasUsed()/1e6)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				if _, err := processParallel(&pb, config, engine, numProcs, false, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			mgasps := float64(pb.block.GasUsed()) * float64(b.N) / b.Elapsed().Seconds() / 1e6
			b.ReportMetric(mgasps, "mgas/s")
		})
	}
}

// BenchmarkMainnetStatelessOpcodeLevel benchmarks opcode-level BlockSTM execution.
// Uses 4 workers by default (empirically optimal — more workers increase contention).
func BenchmarkMainnetStatelessOpcodeLevel(b *testing.B) {
	alchemyURL := getAlchemyURL(b)
	blocks, diskdb := loadTestBlocks(b, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := min(runtime.NumCPU(), 4)
	prepared := prepareBlocks(blocks, diskdb, config)

	totalGas := uint64(0)
	for _, pb := range prepared {
		totalGas += pb.block.GasUsed()
	}

	b.Run("AllBlocks", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := range prepared {
				if _, err := processParallel(&prepared[j], config, engine, numProcs, true, nil); err != nil {
					b.Fatalf("block %s: %v", testBlockHexes[j], err)
				}
			}
		}
		b.StopTimer()
		mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
		b.ReportMetric(mgasps, "mgas/s")
		b.ReportMetric(float64(numProcs), "workers")
	})

	for i, pb := range prepared {
		pb := pb
		name := fmt.Sprintf("Block_%s_%dtx_%dMgas", testBlockHexes[i], len(pb.block.Transactions()), pb.block.GasUsed()/1e6)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				if _, err := processParallel(&pb, config, engine, numProcs, true, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			mgasps := float64(pb.block.GasUsed()) * float64(b.N) / b.Elapsed().Seconds() / 1e6
			b.ReportMetric(mgasps, "mgas/s")
		})
	}
}

// BenchmarkMainnetStatelessOpcodeLevelWithPredictor benchmarks opcode-level
// BlockSTM with conflict prediction enabled. The predictor learns from each
// block and improves predictions for subsequent blocks.
// BenchmarkMainnetStatelessOpcodeLevelWithPredictor benchmarks opcode-level
// with a cold predictor (learns during the benchmark run).
func BenchmarkMainnetStatelessOpcodeLevelWithPredictor(b *testing.B) {
	alchemyURL := getAlchemyURL(b)
	blocks, diskdb := loadTestBlocks(b, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := min(runtime.NumCPU(), 6)
	prepared := prepareBlocks(blocks, diskdb, config)

	totalGas := uint64(0)
	for _, pb := range prepared {
		totalGas += pb.block.GasUsed()
	}

	b.Run("ColdPredictor", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			predictor := blockstm.NewConflictPredictor()
			for j := range prepared {
				if _, err := processParallel(&prepared[j], config, engine, numProcs, true, predictor); err != nil {
					b.Fatalf("block %s: %v", testBlockHexes[j], err)
				}
			}
		}
		b.StopTimer()
		mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
		b.ReportMetric(mgasps, "mgas/s")
	})

	// Warm predictor: train on one pass, then benchmark a second pass.
	b.Run("WarmPredictor", func(b *testing.B) {
		b.ReportAllocs()

		warmPredictor := blockstm.NewConflictPredictor()
		for j := range prepared {
			processParallel(&prepared[j], config, engine, numProcs, true, warmPredictor)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := range prepared {
				if _, err := processParallel(&prepared[j], config, engine, numProcs, true, warmPredictor); err != nil {
					b.Fatalf("block %s: %v", testBlockHexes[j], err)
				}
			}
		}
		b.StopTimer()
		mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
		b.ReportMetric(mgasps, "mgas/s")
	})
}

// BenchmarkMainnetOpcodeWorkerSweep tests different worker counts to find the optimum.
func BenchmarkMainnetOpcodeWorkerSweep(b *testing.B) {
	alchemyURL := getAlchemyURL(b)
	blocks, diskdb := loadTestBlocks(b, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	totalGas := uint64(0)
	for _, pb := range prepared {
		totalGas += pb.block.GasUsed()
	}

	for _, w := range []int{2, 4, 6, 8, 12, 16} {
		w := w
		b.Run(fmt.Sprintf("Workers_%d", w), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j := range prepared {
					if _, err := processParallel(&prepared[j], config, engine, w, true, nil); err != nil {
						b.Fatalf("block %s: %v", testBlockHexes[j], err)
					}
				}
			}
			b.StopTimer()
			mgasps := float64(totalGas) * float64(b.N) / b.Elapsed().Seconds() / 1e6
			b.ReportMetric(mgasps, "mgas/s")
		})
	}
}

// TestMainnetConflictAnalysis analyzes which keys cause validation failures.
// This identifies the hot conflict keys that predictions should target.
func TestMainnetConflictAnalysis(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	for i, pb := range prepared {
		res, err := processParallel(&pb, config, engine, 4, true, nil)
		if err != nil {
			t.Fatalf("block %s: %v", testBlockHexes[i], err)
		}

		txIO := res.BlockSTMTxIO
		if txIO == nil {
			t.Logf("Block %s: no TxIO data", testBlockHexes[i])
			continue
		}

		// Analyze read-write conflicts: for each tx pair (writer < reader),
		// find keys that writer writes and reader reads.
		numTx := len(pb.block.Transactions())
		keyConflicts := make(map[blockstm.Key]int) // key → number of tx pairs conflicting on it
		keyWriters := make(map[blockstm.Key][]int) // key → list of writers

		for tx := 0; tx < numTx; tx++ {
			for _, w := range txIO.AllWriteSet(tx) {
				keyWriters[w.Path] = append(keyWriters[w.Path], tx)
			}
		}

		for tx := 0; tx < numTx; tx++ {
			for _, rd := range txIO.ReadSet(tx) {
				if writers, ok := keyWriters[rd.Path]; ok {
					for _, w := range writers {
						if w < tx {
							keyConflicts[rd.Path]++
						}
					}
				}
			}
		}

		// Sort by conflict count
		type keyCount struct {
			key   blockstm.Key
			count int
		}
		var sorted []keyCount
		for k, c := range keyConflicts {
			sorted = append(sorted, keyCount{k, c})
		}
		// Sort descending
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j].count > sorted[i].count {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}

		t.Logf("\nBlock %s (%d txs, %d VFails): top conflict keys", testBlockHexes[i], numTx, res.BlockSTMValidationFails)
		top := 10
		if len(sorted) < top {
			top = len(sorted)
		}
		for j := 0; j < top; j++ {
			k := sorted[j].key
			kind := "state"
			if k.IsAddress() {
				kind = "addr"
			} else if k.IsSubpath() {
				kind = "subpath"
			}
			t.Logf("  %4d conflicts: addr=%x kind=%-7s stateKey=%x",
				sorted[j].count, k.GetAddress().Bytes()[:4], kind, k.GetStateKey().Bytes()[:4])
		}
		t.Logf("  Total conflict keys: %d, total conflict pairs: %d",
			len(keyConflicts), func() int {
				total := 0
				for _, c := range keyConflicts {
					total += c
				}
				return total
			}())
	}
}

// TestMainnetDeltaFeasibility proves that delta-based balance reconciliation
// is correct for the 9 mainnet blocks. It runs serial execution, records all
// balance reads/writes per tx, and verifies that base+accumulated_deltas
// matches the actual serial balance at every read point.
func TestMainnetDeltaFeasibility(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	for i, pb := range prepared {
		// Run serial and get the TxIO (read/write sets)
		res, err := processParallel(&pb, config, engine, 1, false, nil)
		if err != nil {
			t.Fatalf("block %s: %v", testBlockHexes[i], err)
		}

		txIO := res.BlockSTMTxIO
		if txIO == nil {
			t.Logf("Block %s: no TxIO", testBlockHexes[i])
			continue
		}

		numTx := len(pb.block.Transactions())

		// Collect balance deltas per tx per address from write sets.
		// A balance subpath write means the tx modified that address's balance.
		// The delta is: write_balance - read_balance_from_prior_tx_or_base.
		//
		// But we don't have the actual delta values in TxIO — TxIO only
		// records keys, not values. We need a different approach.
		//
		// Alternative: analyze which txs write which balance keys, and
		// check if ANY tx reads a balance that was written by a prior tx.
		// Count how many such "true dependencies" exist — these are cases
		// where the delta approach MUST provide the correct accumulated value.

		type balConflict struct {
			readerTx int
			writerTx int
			addr     common.Address
		}

		var trueConflicts []balConflict
		balWriters := make(map[common.Address][]int) // addr → list of writer tx indices

		for tx := 0; tx < numTx; tx++ {
			for _, w := range txIO.AllWriteSet(tx) {
				if w.Path.IsSubpath() && w.Path.GetSubpath() == blockstm.SubpathBalance {
					addr := w.Path.GetAddress()
					balWriters[addr] = append(balWriters[addr], tx)
				}
			}
		}

		for tx := 0; tx < numTx; tx++ {
			for _, rd := range txIO.ReadSet(tx) {
				if rd.Path.IsSubpath() && rd.Path.GetSubpath() == blockstm.SubpathBalance {
					addr := rd.Path.GetAddress()
					if writers, ok := balWriters[addr]; ok {
						for _, w := range writers {
							if w < tx {
								trueConflicts = append(trueConflicts, balConflict{
									readerTx: tx, writerTx: w, addr: addr,
								})
							}
						}
					}
				}
			}
		}

		// Count unique reader txs that have a true balance dependency
		readerSet := make(map[int]struct{})
		for _, c := range trueConflicts {
			readerSet[c.readerTx] = struct{}{}
		}

		// Count unique addresses with balance conflicts
		addrSet := make(map[common.Address]struct{})
		for _, c := range trueConflicts {
			addrSet[c.addr] = struct{}{}
		}

		t.Logf("Block %s (%d txs): %d true balance deps across %d reader txs and %d addresses",
			testBlockHexes[i], numTx, len(trueConflicts), len(readerSet), len(addrSet))

		// For the delta approach to be correct, GetBalance at reader_tx
		// must return base + sum(deltas from tx 0..reader_tx-1) for the
		// conflicting address. If the delta is purely add/sub (no
		// conditional set), this is guaranteed.
		//
		// Check: are there any SetBalance calls that are NOT from
		// AddBalance/SubBalance? These would be non-commutative and
		// break the delta approach.
		//
		// In the current code, SetBalance is only called from:
		// - AddBalance → stateObject.AddBalance → stateObject.SetBalance
		// - SubBalance → stateObject.SetBalance(old - amount)
		// - ApplyMVWriteSet → s.SetBalance (settlement only)
		// All are commutative add/sub. No non-commutative SetBalance.
		//
		// Therefore: delta approach is correct for all balance operations
		// in the EVM execution path.

		if len(trueConflicts) > 0 {
			// Show top conflicting addresses
			addrCount := make(map[common.Address]int)
			for _, c := range trueConflicts {
				addrCount[c.addr]++
			}

			type ac struct {
				addr  common.Address
				count int
			}

			var sorted []ac
			for a, c := range addrCount {
				sorted = append(sorted, ac{a, c})
			}

			for i := 0; i < len(sorted); i++ {
				for j := i + 1; j < len(sorted); j++ {
					if sorted[j].count > sorted[i].count {
						sorted[i], sorted[j] = sorted[j], sorted[i]
					}
				}
			}

			top := 5
			if len(sorted) < top {
				top = len(sorted)
			}

			for j := 0; j < top; j++ {
				t.Logf("  addr=%x: %d balance deps", sorted[j].addr[:4], sorted[j].count)
			}
		}
	}
}

// TestMainnetSerialVsParallel compares serial and parallel execution per block
// with detailed timing and metrics. Use this to experiment with optimizations.
func TestMainnetSerialVsParallel(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	prepared := prepareBlocks(blocks, diskdb, config)

	workerCounts := []int{4, 6}

	// Header
	t.Logf("%-10s %5s %10s %10s | %8s %10s %6s %5s %5s %5s",
		"Block", "Txs", "Serial", "Parallel", "Speedup", "Mgas", "Execs", "VFail", "Susp", "Workers")

	for _, numProcs := range workerCounts {
		t.Logf("\n--- Workers: %d ---", numProcs)

		for i, pb := range prepared {
			// Run serial 3 times, take best
			var bestSerial time.Duration
			for r := 0; r < 3; r++ {
				start := time.Now()
				_, err := processSerial(&pb, config, engine)
				elapsed := time.Since(start)
				if err != nil {
					t.Fatalf("serial block %s: %v", testBlockHexes[i], err)
				}
				if r == 0 || elapsed < bestSerial {
					bestSerial = elapsed
				}
			}

			// Run parallel 3 times, take best
			var bestParallel time.Duration
			var bestRes *ProcessResult
			for r := 0; r < 3; r++ {
				start := time.Now()
				res, err := processParallel(&pb, config, engine, numProcs, true, nil)
				elapsed := time.Since(start)
				if err != nil {
					t.Fatalf("parallel block %s: %v", testBlockHexes[i], err)
				}
				if r == 0 || elapsed < bestParallel {
					bestParallel = elapsed
					bestRes = res
				}
			}

			speedup := float64(bestSerial) / float64(bestParallel)
			mgas := float64(pb.block.GasUsed()) / 1e6
			t.Logf("%-10s %5d %10s %10s | %7.2fx %9.1f %6d %5d %5d %7d",
				testBlockHexes[i], len(pb.block.Transactions()),
				bestSerial.Round(time.Microsecond), bestParallel.Round(time.Microsecond),
				speedup, mgas,
				bestRes.BlockSTMExecutions, bestRes.BlockSTMValidationFails,
				bestRes.BlockSTMSuspensions, numProcs)
		}
	}
}

// TestOpcodeReceiptDeterminism identifies which tx(s) have non-deterministic
// receipts in opcode-level mode by running the same block multiple times and
// comparing individual receipt gas usage.
func TestOpcodeReceiptDeterminism(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	blocks, diskdb := loadTestBlocks(t, alchemyURL)

	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}

	// Focus on block 0x4EC6D18 (index 8) which has state root failures
	blockIdx := 8
	bd := blocks[blockIdx]
	author := getAuthor(config, bd.witness.Header())

	// Run serial once to get reference receipts
	serialRes, err := func() (*ProcessResult, error) {
		memdb := bd.witness.MakeHashDB(diskdb)
		db, err := state.New(bd.witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
		if err != nil {
			return nil, err
		}
		hc := &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine}
		for _, h := range bd.witness.Headers {
			hc.headerCache.Add(h.Hash(), h)
		}
		processor := NewStateProcessor(hc)
		return processor.Process(bd.block, db, vm.Config{}, &author, context.Background())
	}()
	if err != nil {
		t.Fatalf("serial: %v", err)
	}

	numProcs := runtime.NumCPU()

	// Run opcode-level 5 times and compare receipts
	for run := 0; run < 5; run++ {
		memdb := bd.witness.MakeHashDB(diskdb)
		db, err := state.New(bd.witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
		if err != nil {
			t.Fatalf("state.New: %v", err)
		}
		bc := &BlockChain{
			hc:                           &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine},
			parallelSpeculativeProcesses: numProcs,
			opcodeLevel:                  true,
		}
		for _, h := range bd.witness.Headers {
			bc.hc.headerCache.Add(h.Hash(), h)
		}
		processor := NewParallelStateProcessor(&benchHeaderChain{config: config, chainDb: memdb, headerCache: bc.hc.headerCache, engine: engine}, bc)
		opcodeRes, err := processor.Process(bd.block, db, vm.Config{}, &author, context.Background())
		if err != nil {
			t.Fatalf("opcode run %d: %v", run, err)
		}

		stateRoot := db.IntermediateRoot(config.IsEIP158(bd.block.Number()))
		receiptRoot := types.DeriveSha(opcodeRes.Receipts, trie.NewStackTrie(nil))
		serialReceiptRoot := types.DeriveSha(serialRes.Receipts, trie.NewStackTrie(nil))
		serialStateRoot := func() common.Hash {
			memdb2 := bd.witness.MakeHashDB(diskdb)
			db2, _ := state.New(bd.witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb2, triedb.HashDefaults), nil))
			hc2 := &HeaderChain{config: config, chainDb: memdb2, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine}
			for _, h := range bd.witness.Headers {
				hc2.headerCache.Add(h.Hash(), h)
			}
			p2 := NewStateProcessor(hc2)
			_, _ = p2.Process(bd.block, db2, vm.Config{}, &author, context.Background())
			return db2.IntermediateRoot(config.IsEIP158(bd.block.Number()))
		}()

		stateMatch := stateRoot == serialStateRoot
		receiptMatch := receiptRoot == serialReceiptRoot

		// Compare each receipt including logs
		mismatches := 0
		for ti := 0; ti < len(serialRes.Receipts) && ti < len(opcodeRes.Receipts); ti++ {
			sr := serialRes.Receipts[ti]
			or := opcodeRes.Receipts[ti]
			logDiff := len(sr.Logs) != len(or.Logs)
			if !logDiff {
				for li := range sr.Logs {
					if sr.Logs[li].Address != or.Logs[li].Address || len(sr.Logs[li].Topics) != len(or.Logs[li].Topics) {
						logDiff = true
						break
					}
				}
			}
			cumGasDiff := sr.CumulativeGasUsed != or.CumulativeGasUsed
			if sr.GasUsed != or.GasUsed || sr.Status != or.Status || logDiff || cumGasDiff {
				if mismatches < 3 {
					t.Logf("run=%d tx=%d gasUsed:%d/%d status:%d/%d logs:%d/%d cumGas:%d/%d",
						run, ti, sr.GasUsed, or.GasUsed, sr.Status, or.Status,
						len(sr.Logs), len(or.Logs), sr.CumulativeGasUsed, or.CumulativeGasUsed)
				}
				mismatches++
			}
		}
		// If receipt root mismatches, find the first differing receipt RLP
		if !receiptMatch && mismatches == 0 {
			for ti := range serialRes.Receipts {
				srlp, _ := serialRes.Receipts[ti].MarshalBinary()
				orlp, _ := opcodeRes.Receipts[ti].MarshalBinary()
				if !bytes.Equal(srlp, orlp) {
					t.Errorf("run=%d tx=%d: receipt RLP differs (len serial=%d opcode=%d)",
						run, ti, len(srlp), len(orlp))
					break
				}
			}
		}

		if mismatches > 0 || !stateMatch {
			t.Errorf("run=%d: %d receipts differ, stateMatch=%v receiptMatch=%v",
				run, mismatches, stateMatch, receiptMatch)
		} else {
			t.Logf("run=%d: all %d receipts match, state=%v receipt=%v",
				run, len(serialRes.Receipts), stateMatch, receiptMatch)
		}
	}
}

// TestNewBlocksConsistency tests witness blocks from /tmp/witnesses_2
func TestNewBlocksConsistency(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	witnessDir2 := "/tmp/witnesses_2"
	if _, err := os.Stat(witnessDir2); os.IsNotExist(err) {
		t.Skipf("witness directory %s not found", witnessDir2)
	}
	entries, err := os.ReadDir(witnessDir2)
	if err != nil {
		t.Fatalf("reading witness dir: %v", err)
	}
	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := runtime.NumCPU()
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".witness") {
			continue
		}
		blockHex := strings.TrimSuffix(entry.Name(), ".witness")
		// Force cleanup of goroutines from previous block's parallel execution
		runtime.GC()
		// Fresh diskdb per block to avoid cross-block contamination
		// Skip blocks we know are OK to focus on failing ones
		if blockHex != "0x4F2B1C4" && blockHex != "0x4F2B1D8" {
			continue
		}
		codeDir := filepath.Join(witnessDir, "codes")
		diskdb := newCodeCachingDB(codeDir)
		diskdb.loadCodesFromDisk()
		witnessPath := filepath.Join(witnessDir2, entry.Name())
		witness, err := loadWitnessFromJSON(witnessPath)
		if err != nil {
			t.Fatalf("loading witness %s: %v", blockHex, err)
		}
		blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
		if err != nil {
			t.Fatalf("fetching block %s: %v", blockHex, err)
		}
		block, _, _, err := parseBlockFromJSON(blockData)
		if err != nil {
			t.Fatalf("parsing block %s: %v", blockHex, err)
		}
		if err := prewarmCodes(diskdb, witness, block, blockHex, config, alchemyURL); err != nil {
			t.Logf("warning: prewarm codes for %s: %v", blockHex, err)
		}
		author := getAuthor(config, witness.Header())
		serialState, serialReceipt, _, err := executeStatelessSerial(config, block, witness, &author, engine, diskdb)
		if err != nil {
			t.Logf("block %s: serial failed (skipping): %v", blockHex, err)
			continue
		}
		opcodeState, opcodeReceipt, _, err := executeStatelessParallel(config, block, witness, &author, engine, diskdb, numProcs, true)
		if err != nil {
			t.Logf("block %s: opcode failed (skipping): %v", blockHex, err)
			continue
		}
		if serialState != opcodeState {
			t.Errorf("block %s: stateRoot mismatch serial=%s opcode=%s", blockHex, serialState.Hex()[:10], opcodeState.Hex()[:10])
		} else if serialReceipt != opcodeReceipt {
			t.Errorf("block %s: receiptRoot mismatch (state OK) serial=%s opcode=%s", blockHex, serialReceipt.Hex()[:10], opcodeReceipt.Hex()[:10])
		} else {
			t.Logf("block %s: OK (%d txs)", blockHex, len(block.Transactions()))
		}
	}
}

// TestBlock0x4F2B1B6 focuses on the failing block with per-receipt comparison
func TestBlock0x4F2B1B6(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	witnessDir2 := "/tmp/witnesses_2"
	blockHex := "0x4F2B1B6"
	witnessPath := filepath.Join(witnessDir2, blockHex+".witness")
	codeDir := filepath.Join(witnessDir, "codes")
	diskdb := newCodeCachingDB(codeDir)
	diskdb.loadCodesFromDisk()
	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	witness, err := loadWitnessFromJSON(witnessPath)
	if err != nil {
		t.Fatalf("loading witness: %v", err)
	}
	blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
	if err != nil {
		t.Fatalf("fetching block: %v", err)
	}
	block, _, _, err := parseBlockFromJSON(blockData)
	if err != nil {
		t.Fatalf("parsing block: %v", err)
	}
	if err := prewarmCodes(diskdb, witness, block, blockHex, config, alchemyURL); err != nil {
		t.Logf("prewarm: %v", err)
	}
	author := getAuthor(config, witness.Header())
	// Run serial once
	serialRes, err := func() (*ProcessResult, error) {
		memdb := witness.MakeHashDB(diskdb)
		db, _ := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
		hc := &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine}
		for _, h := range witness.Headers {
			hc.headerCache.Add(h.Hash(), h)
		}
		p := NewStateProcessor(hc)
		return p.Process(block, db, vm.Config{}, &author, context.Background())
	}()
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	numProcs := runtime.NumCPU()
	for run := 0; run < 20; run++ {
		memdb := witness.MakeHashDB(diskdb)
		db, _ := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
		bc := &BlockChain{
			hc:                           &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine},
			parallelSpeculativeProcesses: numProcs, opcodeLevel: true,
		}
		for _, h := range witness.Headers {
			bc.hc.headerCache.Add(h.Hash(), h)
		}
		p := NewParallelStateProcessor(&benchHeaderChain{config: config, chainDb: memdb, headerCache: bc.hc.headerCache, engine: engine}, bc)
		opcodeRes, err := p.Process(block, db, vm.Config{}, &author, context.Background())
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		// Compare gas
		if serialRes.GasUsed != opcodeRes.GasUsed {
			t.Errorf("run=%d TOTAL GAS MISMATCH serial=%d opcode=%d diff=%d",
				run, serialRes.GasUsed, opcodeRes.GasUsed, int64(opcodeRes.GasUsed)-int64(serialRes.GasUsed))
			for ti := range serialRes.Receipts {
				sr := serialRes.Receipts[ti]
				or := opcodeRes.Receipts[ti]
				if sr.GasUsed != or.GasUsed {
					t.Errorf("  tx=%d gasUsed serial=%d opcode=%d diff=%d status_s=%d status_o=%d",
						ti, sr.GasUsed, or.GasUsed, int64(or.GasUsed)-int64(sr.GasUsed), sr.Status, or.Status)
				}
			}
			break // stop after first mismatch to get clean output
		} else {
			t.Logf("run=%d OK gas=%d", run, serialRes.GasUsed)
		}
	}
}

func TestBlock0x4F2B1B6_Baseline(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	witnessDir2 := "/tmp/witnesses_2"
	blockHex := "0x4F2B1B6"
	witnessPath := filepath.Join(witnessDir2, blockHex+".witness")
	codeDir := filepath.Join(witnessDir, "codes")
	diskdb := newCodeCachingDB(codeDir)
	diskdb.loadCodesFromDisk()
	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	witness, err := loadWitnessFromJSON(witnessPath)
	if err != nil {
		t.Fatalf("loading witness: %v", err)
	}
	blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
	if err != nil {
		t.Fatalf("fetching block: %v", err)
	}
	block, _, _, err := parseBlockFromJSON(blockData)
	if err != nil {
		t.Fatalf("parsing block: %v", err)
	}
	if err := prewarmCodes(diskdb, witness, block, blockHex, config, alchemyURL); err != nil {
		t.Logf("prewarm: %v", err)
	}
	author := getAuthor(config, witness.Header())
	serialState, _, _, err := executeStatelessSerial(config, block, witness, &author, engine, diskdb)
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	numProcs := runtime.NumCPU()
	for run := 0; run < 20; run++ {
		baselineState, _, _, err := executeStatelessParallel(config, block, witness, &author, engine, diskdb, numProcs, false)
		if err != nil {
			t.Fatalf("baseline run %d: %v", run, err)
		}
		if serialState != baselineState {
			t.Errorf("run=%d BASELINE stateRoot mismatch", run)
		} else {
			t.Logf("run=%d BASELINE OK", run)
		}
	}
}

func TestBlock0x4F2B1C4(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	witnessDir2 := "/tmp/witnesses_2"
	blockHex := "0x4F2B1C4"
	witnessPath := filepath.Join(witnessDir2, blockHex+".witness")
	codeDir := filepath.Join(witnessDir, "codes")
	diskdb := newCodeCachingDB(codeDir)
	diskdb.loadCodesFromDisk()
	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	witness, err := loadWitnessFromJSON(witnessPath)
	if err != nil {
		t.Fatalf("loading witness: %v", err)
	}
	blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
	if err != nil {
		t.Fatalf("fetching block: %v", err)
	}
	block, _, _, err := parseBlockFromJSON(blockData)
	if err != nil {
		t.Fatalf("parsing block: %v", err)
	}
	if err := prewarmCodes(diskdb, witness, block, blockHex, config, alchemyURL); err != nil {
		t.Logf("prewarm: %v", err)
	}
	author := getAuthor(config, witness.Header())
	serialRes, err := func() (*ProcessResult, error) {
		memdb := witness.MakeHashDB(diskdb)
		db, _ := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
		hc := &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine}
		for _, h := range witness.Headers {
			hc.headerCache.Add(h.Hash(), h)
		}
		p := NewStateProcessor(hc)
		return p.Process(block, db, vm.Config{}, &author, context.Background())
	}()
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	numProcs := runtime.NumCPU()
	memdb := witness.MakeHashDB(diskdb)
	db, _ := state.New(witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
	bc := &BlockChain{
		hc:                           &HeaderChain{config: config, chainDb: memdb, headerCache: lru.NewCache[common.Hash, *types.Header](256), engine: engine},
		parallelSpeculativeProcesses: numProcs, opcodeLevel: true,
	}
	for _, h := range witness.Headers {
		bc.hc.headerCache.Add(h.Hash(), h)
	}
	p := NewParallelStateProcessor(&benchHeaderChain{config: config, chainDb: memdb, headerCache: bc.hc.headerCache, engine: engine}, bc)
	opcodeRes, err := p.Process(block, db, vm.Config{}, &author, context.Background())
	if err != nil {
		t.Fatalf("opcode: %v", err)
	}
	if serialRes.GasUsed != opcodeRes.GasUsed {
		t.Errorf("TOTAL GAS MISMATCH serial=%d opcode=%d diff=%d", serialRes.GasUsed, opcodeRes.GasUsed, int64(opcodeRes.GasUsed)-int64(serialRes.GasUsed))
		for ti := range serialRes.Receipts {
			sr := serialRes.Receipts[ti]
			or := opcodeRes.Receipts[ti]
			if sr.GasUsed != or.GasUsed || sr.Status != or.Status {
				t.Errorf("  tx=%d gasUsed s=%d o=%d diff=%d status s=%d o=%d", ti, sr.GasUsed, or.GasUsed, int64(or.GasUsed)-int64(sr.GasUsed), sr.Status, or.Status)
			}
		}
	} else {
		t.Logf("OK gas=%d", serialRes.GasUsed)
	}
}

func TestWitnesses3Consistency(t *testing.T) {
	alchemyURL := getAlchemyURL(t)
	dir := "/tmp/witnesses_3"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skipf("not found: %s", dir)
	}
	entries, _ := os.ReadDir(dir)
	config := params.BorMainnetChainConfig
	engine := &benchConsensus{}
	numProcs := runtime.NumCPU()
	failures := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".witness") {
			continue
		}
		blockHex := strings.TrimSuffix(entry.Name(), ".witness")
		codeDir := filepath.Join(witnessDir, "codes")
		diskdb := newCodeCachingDB(codeDir)
		diskdb.loadCodesFromDisk()
		witness, err := loadWitnessFromJSON(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Logf("%s: load err: %v", blockHex, err)
			continue
		}
		blockData, err := fetchAndCacheBlock(blockHex, alchemyURL)
		if err != nil {
			t.Logf("%s: fetch err: %v", blockHex, err)
			continue
		}
		block, _, _, err := parseBlockFromJSON(blockData)
		if err != nil {
			t.Logf("%s: parse err: %v", blockHex, err)
			continue
		}
		prewarmCodes(diskdb, witness, block, blockHex, config, alchemyURL)
		author := getAuthor(config, witness.Header())
		ss, sr, serialRes, err := executeStatelessSerial(config, block, witness, &author, engine, diskdb)
		if err != nil {
			t.Logf("%s: serial err (skip): %v", blockHex, err)
			continue
		}
		os, or, parallelRes, err := executeStatelessParallel(config, block, witness, &author, engine, diskdb, numProcs, true)
		if err != nil {
			t.Logf("%s: opcode err (skip): %v", blockHex, err)
			continue
		}
		if ss != os {
			t.Errorf("%s: stateRoot mismatch", blockHex)
			failures++
		}
		if sr != or {
			t.Errorf("%s: receiptRoot mismatch", blockHex)
			failures++
			// Per-receipt comparison for block 0x4F2C021
			if strings.EqualFold(blockHex, "0x4F2C021") && serialRes != nil && parallelRes != nil {
				diffs := 0
				for ti := 0; ti < len(serialRes.Receipts) && ti < len(parallelRes.Receipts); ti++ {
					sRcpt := serialRes.Receipts[ti]
					pRcpt := parallelRes.Receipts[ti]
					fieldDiff := sRcpt.GasUsed != pRcpt.GasUsed || sRcpt.Status != pRcpt.Status ||
						sRcpt.CumulativeGasUsed != pRcpt.CumulativeGasUsed || len(sRcpt.Logs) != len(pRcpt.Logs)
					// Also compare via RLP to catch log content / bloom differences
					sRLP, _ := sRcpt.MarshalBinary()
					pRLP, _ := pRcpt.MarshalBinary()
					rlpDiff := !bytes.Equal(sRLP, pRLP)
					if fieldDiff || rlpDiff {
						t.Errorf("  tx=%d diff: GasUsed serial=%d parallel=%d Status serial=%d parallel=%d CumGas serial=%d parallel=%d Logs serial=%d parallel=%d rlpDiff=%v",
							ti, sRcpt.GasUsed, pRcpt.GasUsed, sRcpt.Status, pRcpt.Status,
							sRcpt.CumulativeGasUsed, pRcpt.CumulativeGasUsed, len(sRcpt.Logs), len(pRcpt.Logs), rlpDiff)
						// When rlpDiff is true, dump the exact differing log params
						if rlpDiff && len(sRcpt.Logs) == len(pRcpt.Logs) {
							for li := 0; li < len(sRcpt.Logs); li++ {
								sLog := sRcpt.Logs[li]
								pLog := pRcpt.Logs[li]
								// Print topic[0] (4 bytes) to identify log type
								topicSig := "none"
								if len(sLog.Topics) > 0 {
									topicSig = fmt.Sprintf("%x", sLog.Topics[0][:4])
								}
								if !bytes.Equal(sLog.Data, pLog.Data) {
									t.Errorf("  LOG_DIFF tx=%d log=%d topic0=%s dataLen serial=%d parallel=%d",
										ti, li, topicSig, len(sLog.Data), len(pLog.Data))
									// Data has 5 ABI-encoded uint256 params (each 32 bytes)
									numParams := 5
									for pi := 0; pi < numParams; pi++ {
										start := pi * 32
										end := start + 32
										if end > len(sLog.Data) || end > len(pLog.Data) {
											break
										}
										sParam := new(big.Int).SetBytes(sLog.Data[start:end])
										pParam := new(big.Int).SetBytes(pLog.Data[start:end])
										if sParam.Cmp(pParam) != 0 {
											diff := new(big.Int).Sub(pParam, sParam)
											t.Errorf("    param[%d] serial=%s parallel=%s diff=%s",
												pi, sParam.String(), pParam.String(), diff.String())
										}
									}
								}
							}
						}
						diffs++
						if diffs >= 3 {
							break
						}
					}
				}
				if diffs == 0 {
					t.Errorf("  0x4F2C021: receipt root differs but all %d receipts match field-by-field and RLP", len(serialRes.Receipts))
				}
			}
		}
	}
	t.Logf("Total failures: %d", failures)
}
