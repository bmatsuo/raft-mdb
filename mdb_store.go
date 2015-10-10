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
	dbLogs  lmdb.DBI
	dbConf  lmdb.DBI
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
	return m.env.Update(func(txn *lmdb.Txn) (err error) {
		m.dbLogs, err = txn.OpenDBI(dbLogs, lmdb.Create)
		if err != nil {
			return err
		}
		m.dbConf, err = txn.OpenDBI(dbConf, lmdb.Create)
		if err != nil {
			return err
		}
		return nil
	})
}

func lmdbNop([]lmdb.DBI) lmdb.TxnOp { return func(_ *lmdb.Txn) error { return nil } }

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	m.env.Close()
	return nil
}

func (m *MDBStore) FirstIndex() (uint64, error) {
	return m.getIndex(nil, nil, lmdb.First)
}

func (m *MDBStore) LastIndex() (uint64, error) {
	return m.getIndex(nil, nil, lmdb.Last)
}

func (m *MDBStore) getIndex(k, v []byte, op uint) (uint64, error) {
	var k64 uint64
	err := m.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true

		cursor, err := txn.OpenCursor(m.dbLogs)
		if err != nil {
			return err
		}

		k, _, err = cursor.Get(nil, nil, op)
		cursor.Close()

		if lmdb.IsNotFound(err) {
			return nil
		} else if err == nil {
			k64 = bytesToUint64(k)
		}

		return err
	})
	return k64, err
}

// Gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *raft.Log) error {
	key := uint64ToBytes(index)
	return m.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true

		val, err := txn.Get(m.dbLogs, key)
		if lmdb.IsNotFound(err) {
			return raft.ErrLogNotFound
		} else if err != nil {
			return err
		}

		// Convert the value to a log
		return decodeMsgPack(val, logOut)
	})

}

// Stores a log entry
func (m *MDBStore) StoreLog(log *raft.Log) error {
	return m.StoreLogs([]*raft.Log{log})
}

// Stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*raft.Log) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) error {
		txn.RawRead = true

		for _, log := range logs {
			// Convert to an on-disk format
			key := uint64ToBytes(log.Index)
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}

			// Write to the table
			if err := txn.Put(m.dbLogs, key, val.Bytes(), 0); err != nil {
				return err
			}
		}

		return nil
	})
}

// Deletes a range of log entries. The range is inclusive.
func (m *MDBStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true

		// Hack around an LMDB bug by running the delete multiple
		// times until there are no further rows.
		var num int
		for cont := true; cont; cont = num <= 0 {
			num, err = m.innerDeleteRange(txn, m.dbLogs, minIdx, maxIdx)
			if err != nil {
				return err
			}
		}

		return nil
	})

}

// innerDeleteRange does a single pass to delete the indexes (inclusively)
func (m *MDBStore) innerDeleteRange(txn *lmdb.Txn, dbi lmdb.DBI, minIdx, maxIdx uint64) (num int, err error) {
	s := lmdbscan.New(txn, dbi)
	defer s.Close()

	s.Set(uint64ToBytes(minIdx), nil, lmdb.SetKey)
	for s.Scan() {
		if maxIdx < bytesToUint64(s.Key()) {
			break
		}
		if err := s.Del(0); err != nil {
			return num, err
		}
		num++
	}
	if err := s.Err(); err != nil {
		return num, err
	}
	return num, nil
}

// Set a K/V pair
func (m *MDBStore) Set(key []byte, val []byte) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) error { return txn.Put(m.dbConf, key, val, 0) })
}

// Get a K/V pair
func (m *MDBStore) Get(key []byte) ([]byte, error) {
	var val []byte
	// Start read txn
	err := m.env.View(func(txn *lmdb.Txn) (err error) {
		val, err = txn.Get(m.dbConf, key)
		if lmdb.IsNotFound(err) {
			return fmt.Errorf("not found")
		}
		return err
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
