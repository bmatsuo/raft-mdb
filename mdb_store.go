package raftmdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bmatsuo/lmdb-go/exp/lmdbpool"
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
	// Determines how frequently the MDBStore will check for stale readers.
	// ReaderCheck is guaranteed to be called between any two updates more than
	// ReaderCheckPeriod apart.  If ReaderCheckPeriod is the MDBStore will only
	// check for stale readers while initializing.  ReaderCheckPeriod only
	// needs to be non-zero if other applications read from the backing
	// database concurrently.
	ReaderCheckPeriod time.Duration

	env     *lmdb.Env
	path    string
	maxSize int64
	dbLogs  lmdb.DBI
	dbConf  lmdb.DBI

	// bufu64 is a small buffer that can be used to store encoded uint64 values
	// during updates.  bufu64 cannot be used during view transactions because
	// they may execute concurrent with other transactions.
	bufu64 []byte

	readersChecked time.Time

	txnPool       *lmdbpool.TxnPool
	logCursorPool *sync.Pool
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
	err := m.env.SetMaxDBs(2)
	if err != nil {
		return err
	}

	// Increase the maximum map size
	err = m.env.SetMapSize(m.maxSize)
	if err != nil {
		return err
	}

	// Open the DB
	err = m.env.Open(m.path, 0, 0755)
	if err != nil {
		m.env.Close()
		return err
	}

	// Always check for stale readers when initializing an MDBStore, whether to
	// check again after initialization is governed by the m.ReaderCheckPeriod.
	_, err = m.checkStaleReaders(true)
	if err != nil {
		return err
	}

	// Initialize pools and open all DBIs ('tables') that will be needed during
	// the lifetime of the MDBStore.
	m.logCursorPool = &sync.Pool{}
	m.txnPool = lmdbpool.NewTxnPool(m.env)
	err = m.txnPool.Update(func(txn *lmdb.Txn) (err error) {
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
	if err != nil {
		m.env.Close()
		return err
	}

	return nil
}

// Close is used to gracefully shutdown the MDB store
func (m *MDBStore) Close() error {
	// Clear pools before closing the environment to be sure that we don't
	// potentially leak C resources which cannot be collected.
	for {
		cur, ok := m.logCursorPool.Get().(*lmdb.Cursor)
		if ok {
			cur.Close()
		} else {
			break
		}
	}
	m.txnPool.Close()
	m.env.Close()
	return nil
}

func (m *MDBStore) logsCursor(txn *lmdb.Txn) (*lmdb.Cursor, error) {
	cur, ok := m.logCursorPool.Get().(*lmdb.Cursor)
	if !ok {
		return txn.OpenCursor(m.dbLogs)
	}
	err := cur.Renew(txn)
	if err != nil {
		return txn.OpenCursor(m.dbLogs)
	}
	return cur, nil
}

// It is important to periodically call Env.ReaderCheck() to prevent
// unnecessary database growth.  While transactions should clean up after
// themselves, program crashes can leave reader slots tied to dead programs
// without calling this function.
//
// m.env.ReaderCheck runs quickly, but you wouldn't want to call it every
// second.
func (m *MDBStore) checkStaleReaders(force bool) (int, error) {
	if !force && m.ReaderCheckPeriod == 0 {
		return 0, nil
	}

	if m.readersChecked.IsZero() || time.Since(m.readersChecked) > m.ReaderCheckPeriod {
		m.readersChecked = time.Now()
		return m.env.ReaderCheck()
	}
	return 0, nil
}

// FirstIndex returns the first index in the MDBStore
func (m *MDBStore) FirstIndex() (uint64, error) {
	return m.getIndex(lmdb.First)
}

// LastIndex returns the last index in the MDBStore
func (m *MDBStore) LastIndex() (uint64, error) {
	return m.getIndex(lmdb.Last)
}

func (m *MDBStore) getIndex(op uint) (uint64, error) {
	txn, err := m.txnPool.BeginTxn(lmdb.Readonly)
	if err != nil {
		return 0, err
	}
	defer m.txnPool.Abort(txn)

	txn.RawRead = true

	cur, err := m.logsCursor(txn)
	if err != nil {
		return 0, err
	}
	// NOTE:
	// The release of the cursor is not deferred because a runtime panic would
	// not be catastrophic.  The txn, however, must be be reset to avoid stale
	// readers causing the database to grow.

	k, _, err := cur.Get(nil, nil, op)
	m.logCursorPool.Put(cur)
	if err == nil {
		return bytesToUint64(k), nil
	}
	if lmdb.IsNotFound(err) {
		return 0, nil
	}
	return 0, err
}

// GetLog gets a log entry at a given index
func (m *MDBStore) GetLog(index uint64, logOut *raft.Log) error {
	txn, err := m.txnPool.BeginTxn(lmdb.Readonly)
	if err != nil {
		return err
	}
	defer m.txnPool.Abort(txn)

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
}

// StoreLog stores a log entry
func (m *MDBStore) StoreLog(log *raft.Log) error {
	_, err := m.checkStaleReaders(false)
	if err != nil {
		return err
	}

	return m.txnPool.Update(func(txn *lmdb.Txn) error {
		val := bytes.NewBuffer(nil)
		return m.putLog(txn, log, val)
	})
}

// StoreLogs stores multiple log entries
func (m *MDBStore) StoreLogs(logs []*raft.Log) error {
	_, err := m.checkStaleReaders(false)
	if err != nil {
		return err
	}

	// Start write txn
	return m.txnPool.Update(func(txn *lmdb.Txn) error {
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
	_, err := m.checkStaleReaders(false)
	if err != nil {
		return err
	}

	// Start write txn
	return m.txnPool.Update(func(txn *lmdb.Txn) (err error) {
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
	_, err := m.checkStaleReaders(false)
	if err != nil {
		return err
	}

	return m.txnPool.Update(func(txn *lmdb.Txn) error {
		return txn.Put(m.dbConf, key, val, 0)
	})
}

// SetUint64 sets a key to a uint64 value.
func (m *MDBStore) SetUint64(key []byte, val uint64) error {
	_, err := m.checkStaleReaders(false)
	if err != nil {
		return err
	}

	return m.txnPool.Update(func(txn *lmdb.Txn) error {
		m.bufu64 = uint64ToBytes(m.bufu64[:0], val)
		return txn.Put(m.dbConf, key, m.bufu64, 0)
	})
}

// Get returns the value for a given key.
func (m *MDBStore) Get(key []byte) ([]byte, error) {
	txn, err := m.txnPool.BeginTxn(lmdb.Readonly)
	if err != nil {
		return nil, err
	}
	defer m.txnPool.Abort(txn)

	val, err := txn.Get(m.dbConf, key)
	if err == nil {
		return val, nil
	}
	if lmdb.IsNotFound(err) {
		return nil, fmt.Errorf("not found")
	}
	return nil, err
}

// GetUint64 returns the value of a key interpreted as a uint64 value.
func (m *MDBStore) GetUint64(key []byte) (v64 uint64, err error) {
	txn, err := m.txnPool.BeginTxn(lmdb.Readonly)
	if err != nil {
		return 0, err
	}
	defer m.txnPool.Abort(txn)

	txn.RawRead = true

	val, err := txn.Get(m.dbConf, key)
	if err == nil {
		return bytesToUint64(val), nil
	}
	if lmdb.IsNotFound(err) {
		return 0, fmt.Errorf("not found")
	}
	return 0, err
}
