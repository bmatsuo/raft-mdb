package raftmdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
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

	// bufu64 is a small buffer that can be used to store encoded uint64 values
	// during updates.  bufu64 cannot be used during view transactions because
	// they may execute concurrent with other transactions.
	bufu64 []byte
}

// NewMDBStore returns a new MDBStore and potential
// error. Requres a base directory from which to operate.
// Uses the default maximum size.
func NewMDBStore(base string) (*MDBStore, error) {
	return NewMDBStoreWithSize(base, 0)
}

// NewMDBStoreWithSize returns a new MDBStore and potential
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

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	m.env.Close()
	return nil
}

// FirstIndex returns the first index in the MDBStore
func (m *MDBStore) FirstIndex() (uint64, error) {
	return m.getIndex(nil, nil, lmdb.First)
}

// LastIndex returns the last index in the MDBStore
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

// GetLog gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *raft.Log) error {
	return m.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true

		key := uint64ToBytes(nil, index)
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

// StoreLog stores a log entry
func (m *MDBStore) StoreLog(log *raft.Log) error {
	return m.env.Update(func(txn *lmdb.Txn) error {
		val := bytes.NewBuffer(nil)
		return m.putLog(txn, log, val)
	})
}

// StoreLogs stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*raft.Log) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) error {
		val := bytes.NewBuffer(nil)
		for _, log := range logs {
			err := m.putLog(txn, log, val)
			if err != nil {
				return err
			}
			val.Reset()
		}

		return nil
	})
}

func (m *MDBStore) putLog(txn *lmdb.Txn, log *raft.Log, val *bytes.Buffer) error {
	// Convert to an on-disk format
	m.bufu64 = uint64ToBytes(m.bufu64[:0], log.Index)
	err := encodeMsgPack(val, log)
	if err != nil {
		return err
	}

	// Write to the table
	return txn.Put(m.dbLogs, m.bufu64, val.Bytes(), 0)
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (m *MDBStore) DeleteRange(minIdx, maxIdx uint64) error {
	// Start write txn
	return m.env.Update(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		s := lmdbscan.New(txn, m.dbLogs)
		defer s.Close()
		m.bufu64 = uint64ToBytes(m.bufu64[:0], minIdx)
		s.Set(m.bufu64, nil, lmdb.SetKey)
		for s.Scan() {
			if maxIdx < bytesToUint64(s.Key()) {
				break
			}
			if err := s.Cursor().Del(0); err != nil {
				return err
			}
		}
		return s.Err()
	})
}

// Set associates a key with a value.
func (m *MDBStore) Set(key, val []byte) error {
	return m.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(m.dbConf, key, val, 0)
	})
}

// SetUint64 sets a key to a uint64 value.
func (m *MDBStore) SetUint64(key []byte, val uint64) error {
	return m.env.Update(func(txn *lmdb.Txn) error {
		m.bufu64 = uint64ToBytes(m.bufu64[:0], val)
		return txn.Put(m.dbConf, key, m.bufu64, 0)
	})
}

// Get returns the value for a given key.
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

// GetUint64 returns the value of a key interpreted as a uint64 value.
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
