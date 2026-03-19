package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// cachingReader wraps a Reader with a thread-safe account cache.
// Used during parallel execution: the first tx to read an account from
// the trie populates the cache, and subsequent txs read from cache.
// Safe because account data from the base trie is immutable within a block.
type cachingReader struct {
	inner    Reader
	accounts sync.Map // common.Address → *cachedAccount
}

type cachedAccount struct {
	acct *types.StateAccount
	err  error
}

func NewCachingReader(inner Reader) Reader {
	return &cachingReader{inner: inner}
}

func (r *cachingReader) Account(addr common.Address) (*types.StateAccount, error) {
	if v, ok := r.accounts.Load(addr); ok {
		ca := v.(*cachedAccount)

		if ca.acct == nil {
			return nil, ca.err
		}

		// Return a copy (the contract says "safe to modify after the call")
		cpy := *ca.acct
		return &cpy, ca.err
	}

	acct, err := r.inner.Account(addr)

	r.accounts.Store(addr, &cachedAccount{acct: acct, err: err})

	if acct == nil {
		return nil, err
	}

	// Return a copy for the caller, keep original in cache
	cpy := *acct
	return &cpy, err
}

func (r *cachingReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	return r.inner.Storage(addr, slot)
}

// PreWarmReader populates a Reader's cache with the given addresses.
func PreWarmReader(reader Reader, addrs []common.Address) {
	for _, addr := range addrs {
		reader.Account(addr)
	}
}

func (r *cachingReader) Code(addr common.Address, codeHash common.Hash) ([]byte, error) {
	return r.inner.Code(addr, codeHash)
}

func (r *cachingReader) CodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return r.inner.CodeSize(addr, codeHash)
}
