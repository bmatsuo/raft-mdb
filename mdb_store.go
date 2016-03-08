package raftmdb

import (
	"fmt"
	"os"
	"path/filepath"

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
		m.dbLogs, err = txn.OpenDBI(dbLogs, lmdb.Create|lmdb.IntegerKey)
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

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	m.env.Close()
	return nil
}

// FirstIndex returns the first index
func (m *MDBStore) FirstIndex() (uint64, error) {
	return m.getIndex(nil, nil, lmdb.First)
}

// LastIndex returns the last index
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
			kp, ok := lmdb.UintptrValue(k)
			if !ok {
				panic("key size")
			}
			k64 = uint64(kp)
		}

		return err
	})
	return k64, err
}

// GetLog gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *raft.Log) error {
	return m.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true

		val, err := txn.GetValue(m.dbLogs, lmdb.Uintptr(uintptr(index)))
		if lmdb.IsNotFound(err) {
			return raft.ErrLogNotFound
		} else if err != nil {
			return err
		}

		// Convert the value to a log
		return decodeMsgPack(val, logOut)
	})

}

// StoreLog stores a log entry
func (m *MDBStore) StoreLog(log *raft.Log) error {
	return m.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*raft.Log) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) error {
		for _, log := range logs {
			// Convert to an on-disk format
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}

			// Write to the table
			k := lmdb.Uintptr(uintptr(log.Index))
			v := lmdb.Bytes(val.Bytes())
			if err := txn.PutValue(m.dbLogs, k, v, 0); err != nil {
				return err
			}
		}

		return nil
	})
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (m *MDBStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		cur, err := txn.OpenCursor(m.dbLogs)
		if err != nil {
			return err
		}
		start := lmdb.Uintptr(uintptr(minIdx))
		for k, _, err := cur.GetValue(start, lmdb.Bytes(nil), lmdb.SetKey); ; k, _, err = cur.GetValue(lmdb.Bytes(nil), lmdb.Bytes(nil), lmdb.Next) {
			if err != nil {
				if lmdb.IsNotFound(err) {
					return nil
				}
				return err
			}

			idx, ok := lmdb.UintptrValue(k)
			if !ok {
				panic("key size")
			}
			if maxIdx < uint64(idx) {
				return nil
			}
			if err := cur.Del(0); err != nil {
				return err
			}
		}
	})
}

// Set a K/V pair
func (m *MDBStore) Set(key, val []byte) error {
	return m.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(m.dbConf, key, val, 0)
	})
}

func (m *MDBStore) SetUint64(key []byte, val uint64) error {
	return m.Set(key, uint64ToBytes(val))
}

// Get a K/V pair
func (m *MDBStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := m.env.View(func(txn *lmdb.Txn) (err error) {
		val, err = txn.Get(m.dbConf, key)
		if lmdb.IsNotFound(err) {
			return fmt.Errorf("not found")
		}
		return err
	})
	return val, err
}

func (m *MDBStore) GetUint64(key []byte) (v64 uint64, err error) {
	err = m.env.View(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		val, err := txn.Get(m.dbConf, key)
		if lmdb.IsNotFound(err) {
			return fmt.Errorf("not found")
		}
		if err == nil {
			v64 = bytesToUint64(val)
		}
		return err
	})
	return v64, err
}
