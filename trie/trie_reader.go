// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
<<<<<<< Updated upstream
=======
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/trie/triestate"
>>>>>>> Stashed changes
)

var trieNodeMeter = metrics.NewRegisteredMeter("trie/node/read", nil)
var trieNodeMeter10 = metrics.NewRegisteredMeter("trie/node/read10", nil)
var trieNodeMeter100 = metrics.NewRegisteredMeter("trie/node/read100", nil)
var trieNodeMeter1000 = metrics.NewRegisteredMeter("trie/node/read1000", nil)
var trieNodeMeter10000 = metrics.NewRegisteredMeter("trie/node/read10000", nil)
var trieNodeMeter100000 = metrics.NewRegisteredMeter("trie/node/read100000", nil)
var trieNodeMeter1000000 = metrics.NewRegisteredMeter("trie/node/read1000000", nil)
var trieNodeMeter10000000 = metrics.NewRegisteredMeter("trie/node/read10000000", nil)

// Reader wraps the Node method of a backing trie store.
type Reader interface {
	// Node retrieves the trie node blob with the provided trie identifier, node path and
	// the corresponding node hash. No error will be returned if the node is not found.
	//
	// When looking up nodes in the account trie, 'owner' is the zero hash. For contract
	// storage trie nodes, 'owner' is the hash of the account address that containing the
	// storage.
	//
	// TODO(rjl493456442): remove the 'hash' parameter, it's redundant in PBSS.
	Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error)
}

// trieReader is a wrapper of the underlying node reader. It's not safe
// for concurrent usage.
type trieReader struct {
	owner      common.Hash
	reader     Reader
	banned     map[string]struct{} // Marker to prevent node from being accessed, for tests
	nodeHitMap map[string]int
}

// newTrieReader initializes the trie reader with the given node reader.
func newTrieReader(stateRoot, owner common.Hash, db *Database) (*trieReader, error) {
	if stateRoot == (common.Hash{}) || stateRoot == types.EmptyRootHash {
		if stateRoot == (common.Hash{}) {
			log.Error("Zero state root hash!")
		}
		return &trieReader{owner: owner, nodeHitMap: make(map[string]int)}, nil
	}
	reader, err := db.Reader(stateRoot)
	if err != nil {
		return nil, &MissingNodeError{Owner: owner, NodeHash: stateRoot, err: err}
	}

	return &trieReader{owner: owner, reader: reader, nodeHitMap: make(map[string]int)}, nil
}

// newEmptyReader initializes the pure in-memory reader. All read operations
// should be forbidden and returns the MissingNodeError.
func newEmptyReader() *trieReader {
	return &trieReader{}
}

// node retrieves the rlp-encoded trie node with the provided trie node
// information. An MissingNodeError will be returned in case the node is
// not found or any error is encountered.
func (r *trieReader) node(path []byte, hash common.Hash) ([]byte, error) {
	// Perform the logics in tests for preventing trie node access.
	if r.banned != nil {
		if _, ok := r.banned[string(path)]; ok {
			return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
		}
	}

	if r.reader == nil {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path}
	}
	blob, err := r.reader.Node(r.owner, path, hash)
	if err != nil || len(blob) == 0 {
		return nil, &MissingNodeError{Owner: r.owner, NodeHash: hash, Path: path, err: err}
	}

	var fullPath string

	if r.owner == (common.Hash{}) {
		fullPath = string(path)
	} else {
		fullPath = r.owner.Hex() + string(common.Bytes2Hex(path))
	}

	if r.nodeHitMap[fullPath] == 0 {
		trieNodeMeter.Mark(1)
	}

	r.nodeHitMap[fullPath] += 1

	if r.nodeHitMap[fullPath] == 10 {
		trieNodeMeter10.Mark(1)
	} else if r.nodeHitMap[fullPath] == 100 {
		trieNodeMeter100.Mark(1)
	} else if r.nodeHitMap[fullPath] == 1000 {
		trieNodeMeter1000.Mark(1)
	} else if r.nodeHitMap[fullPath] == 10000 {
		trieNodeMeter10000.Mark(1)
	} else if r.nodeHitMap[fullPath] == 100000 {
		trieNodeMeter100000.Mark(1)
	} else if r.nodeHitMap[fullPath] == 1000000 {
		trieNodeMeter1000000.Mark(1)
	} else if r.nodeHitMap[fullPath] == 10000000 {
		trieNodeMeter10000000.Mark(1)
	}

	return blob, nil
}
