package raftmdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/hashicorp/raft"
)

const (
	dbLogs       = "logs"
	dbConf       = "conf"
	dbMaxMapSize = 128 * 1024 * 1024 // 128MB default max map size
)

// Sub-dir used for MDB
var mdbPath = "mdb/"

// MDBStore provides an implementation of LogStore and StableStore,
// all backed by a single MDB database.
type MDBStore struct {
	env     *lmdb.Env
	path    string
	maxSize int64
}

// NewMDBStore returns a new MDBStore and potential
// error. Requres a base directory from which to operate.
// Uses the default maximum size.
func NewMDBStore(base string) (*MDBStore, error) {
	return NewMDBStoreWithSize(base, 0)
}

// NewMDBStore returns a new MDBStore and potential
// error. Requres a base directory from which to operate,
// and a maximum size. If maxSize is not 0, a default value is used.
func NewMDBStoreWithSize(base string, maxSize int64) (*MDBStore, error) {
	// Get the paths
	path := filepath.Join(base, mdbPath)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Set the maxSize if not given
	if maxSize == 0 {
		maxSize = dbMaxMapSize
	}

	// Create the env
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	// Create the struct
	store := &MDBStore{
		env:     env,
		path:    path,
		maxSize: maxSize,
	}

	// Initialize the db
	if err := store.initialize(); err != nil {
		env.Close()
		return nil, err
	}
	return store, nil
}

// initialize is used to setup the mdb store
func (m *MDBStore) initialize() error {
	// Allow up to 2 sub-dbs
	if err := m.env.SetMaxDBs(2); err != nil {
		return err
	}

	// Increase the maximum map size
	if err := m.env.SetMapSize(m.maxSize); err != nil {
		return err
	}

	// Open the DB
	if err := m.env.Open(m.path, 0, 0755); err != nil {
		return err
	}

	// Create all the tables
	return m.runTxn(false, []string{dbLogs, dbConf}, lmdbNop)
}

func lmdbNop([]lmdb.DBI) lmdb.TxnOp { return func(_ *lmdb.Txn) error { return nil } }

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	m.env.Close()
	return nil
}

// startTxn is used to start a transaction and open all the associated sub-databases
func (m *MDBStore) runTxn(readonly bool, open []string, fn func([]lmdb.DBI) lmdb.TxnOp) error {
	var txFlags uint = 0
	var dbFlags uint = 0
	if readonly {
		txFlags |= lmdb.Readonly
	} else {
		dbFlags |= lmdb.Create
	}

	err := m.env.RunTxn(txFlags, func(txn *lmdb.Txn) (err error) {
		var dbs []lmdb.DBI
		for _, name := range open {
			dbi, err := txn.OpenDBI(name, dbFlags)
			if err != nil {
				return nil
			}
			dbs = append(dbs, dbi)
		}
		return fn(dbs)(txn)
	})
	return err
}

func (m *MDBStore) FirstIndex() (uint64, error) {
	_, _, index, err := m.getIndex(nil, nil, lmdb.First)
	return index, err
}

func (m *MDBStore) LastIndex() (uint64, error) {
	_, _, index, err := m.getIndex(nil, nil, lmdb.Last)
	return index, err
}

func (m *MDBStore) getIndex(k, v []byte, op uint) ([]byte, []byte, uint64, error) {
	var _k, _v []byte
	var k64 uint64
	err := m.runTxn(true, []string{dbLogs}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) error {
			txn.RawRead = true

			cursor, err := txn.OpenCursor(dbis[0])
			if err != nil {
				return err
			}
			defer cursor.Close()

			_k, _v, err = cursor.Get(nil, nil, op)
			if lmdb.IsNotFound(err) {
				return nil
			} else if err == nil {
				k64 = bytesToUint64(_k)
			}

			return err
		}
	})
	return _k, _v, k64, err
}

// Gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *raft.Log) error {
	key := uint64ToBytes(index)
	return m.runTxn(true, []string{dbLogs}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) error {
			txn.RawRead = true

			val, err := txn.Get(dbis[0], key)
			if lmdb.IsNotFound(err) {
				return raft.ErrLogNotFound
			} else if err != nil {
				return err
			}

			// Convert the value to a log
			return decodeMsgPack(val, logOut)
		}
	})

}

// Stores a log entry
func (m *MDBStore) StoreLog(log *raft.Log) error {
	return m.StoreLogs([]*raft.Log{log})
}

// Stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*raft.Log) error {
	// Start write txn
	return m.runTxn(false, []string{dbLogs}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) error {
			txn.RawRead = true

			for _, log := range logs {
				// Convert to an on-disk format
				key := uint64ToBytes(log.Index)
				val, err := encodeMsgPack(log)
				if err != nil {
					return err
				}

				// Write to the table
				if err := txn.Put(dbis[0], key, val.Bytes(), 0); err != nil {
					return err
				}
			}

			return nil
		}
	})
}

// Deletes a range of log entries. The range is inclusive.
func (m *MDBStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Start write txn
	return m.runTxn(false, []string{dbLogs}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) (err error) {
			txn.RawRead = true

			// Hack around an LMDB bug by running the delete multiple
			// times until there are no further rows.
			var num int
			for cont := true; cont; cont = num <= 0 {
				num, err = m.innerDeleteRange(txn, dbis, minIdx, maxIdx)
				if err != nil {
					return err
				}
			}

			return nil
		}
	})

}

// innerDeleteRange does a single pass to delete the indexes (inclusively)
func (m *MDBStore) innerDeleteRange(txn *lmdb.Txn, dbis []lmdb.DBI, minIdx, maxIdx uint64) (num int, err error) {
	k := uint64ToBytes(minIdx)
	scanner := lmdbscan.New(txn, dbis[0])
	scanner.Set(k, nil, lmdb.SetKey)
	for scanner.Scan() {
		if maxIdx < bytesToUint64(scanner.Key()) {
			break
		}
		if err := scanner.Del(0); err != nil {
			return num, err
		}
		num++
	}
	if err := scanner.Err(); err != nil {
		return num, err
	}
	return num, nil
}

// Set a K/V pair
func (m *MDBStore) Set(key []byte, val []byte) error {
	// Start write txn
	return m.runTxn(false, []string{dbConf}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) error { return txn.Put(dbis[0], key, val, 0) }
	})
}

// Get a K/V pair
func (m *MDBStore) Get(key []byte) ([]byte, error) {
	var val []byte
	// Start read txn
	err := m.runTxn(true, []string{dbConf}, func(dbis []lmdb.DBI) lmdb.TxnOp {
		return func(txn *lmdb.Txn) (err error) {
			val, err = txn.Get(dbis[0], key)
			if lmdb.IsNotFound(err) {
				return fmt.Errorf("not found")
			}
			return err
		}
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (m *MDBStore) SetUint64(key []byte, val uint64) error {
	return m.Set(key, uint64ToBytes(val))
}

func (m *MDBStore) GetUint64(key []byte) (uint64, error) {
	buf, err := m.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(buf), nil
}
