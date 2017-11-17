// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdb

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/roasbeef/btcwallet/walletdb"
)

var database db

var mtx sync.Mutex

// convertErr converts some bolt errors to the equivalent walletdb error.
func convertErr(err error) error {
	switch err {
	// Database open/create errors.
	case bolt.ErrDatabaseNotOpen:
		return walletdb.ErrDbNotOpen
	case bolt.ErrInvalid:
		return walletdb.ErrInvalid

	// Transaction errors.
	case bolt.ErrTxNotWritable:
		return walletdb.ErrTxNotWritable
	case bolt.ErrTxClosed:
		return walletdb.ErrTxClosed

	// Value/bucket errors.
	case bolt.ErrBucketNotFound:
		return walletdb.ErrBucketNotFound
	case bolt.ErrBucketExists:
		return walletdb.ErrBucketExists
	case bolt.ErrBucketNameRequired:
		return walletdb.ErrBucketNameRequired
	case bolt.ErrKeyRequired:
		return walletdb.ErrKeyRequired
	case bolt.ErrKeyTooLarge:
		return walletdb.ErrKeyTooLarge
	case bolt.ErrValueTooLarge:
		return walletdb.ErrValueTooLarge
	case bolt.ErrIncompatibleValue:
		return walletdb.ErrIncompatibleValue
	}

	// Return the original error if none of the above applies.
	return err
}

// transaction represents a database transaction.  It can either by read-only or
// read-write and implements the walletdb Tx interfaces.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	m *db
}

func (tx *transaction) ReadBucket(key []byte) walletdb.ReadBucket {
	//fmt.Println("getting readbucket", string(key))
	return tx.ReadWriteBucket(key)
}

func (tx *transaction) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	//fmt.Println("getting bucket", string(key))
	buc := (*tx.m)[string(key)]
	if buc == nil {
		return nil
	}
	m, ok := buc.(map[string]interface{})
	if !ok {
		return nil
	}
	//fmt.Println("found bucket", m, len(m))
	return (*bucket)(&m)
}

func (tx *transaction) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	fmt.Println("creting top level bucket", string(key))

	_, ok := (*tx.m)[string(key)]
	if ok {
		return nil, walletdb.ErrBucketExists
	}
	buc := make(map[string]interface{})
	(*tx.m)[string(key)] = buc
	return (*bucket)(&buc), nil
}

func (tx *transaction) DeleteTopLevelBucket(key []byte) error {
	fmt.Println("deletetop level bucket", string(key))
	delete((*tx.m), string(key))
	return nil
}

// Commit commits all changes that have been made through the root bucket and
// all of its sub-buckets to persistent storage.
//
// This function is part of the walletdb.Tx interface implementation.
func (tx *transaction) Commit() error {
	mtx.Unlock()
	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the walletdb.Tx interface implementation.
func (tx *transaction) Rollback() error {
	mtx.Unlock()
	return nil
}

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the walletdb Bucket interfaces.
type bucket map[string]interface{}

// Enforce bucket implements the walletdb Bucket interfaces.
var _ walletdb.ReadWriteBucket = (*bucket)(nil)

// NestedReadWriteBucket retrieves a nested bucket with the given key.  Returns
// nil if the bucket does not exist.
//
// This function is part of the walletdb.ReadWriteBucket interface implementation.
func (b *bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	fmt.Println("nested bucket", string(key))
	buc, ok := (*b)[string(key)]
	if !ok {
		return nil
	}
	m, ok := buc.(map[string]interface{})
	if !ok {
		return nil
	}
	return (*bucket)(&m)
}

func (b *bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	fmt.Println("nested read bucket", string(key))
	return b.NestedReadWriteBucket(key)
}

// CreateBucket creates and returns a new nested bucket with the given key.
// Returns ErrBucketExists if the bucket already exists, ErrBucketNameRequired
// if the key is empty, or ErrIncompatibleValue if the key value is otherwise
// invalid.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	//fmt.Println("creting bucet", string(key))
	_, ok := (*b)[string(key)]
	if ok {
		return nil, walletdb.ErrBucketExists
	}
	buc := make(map[string]interface{})
	(*b)[string(key)] = buc
	return (*bucket)(&buc), nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.  Returns ErrBucketNameRequired if the
// key is empty or ErrIncompatibleValue if the key value is otherwise invalid.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, error) {
	//fmt.Println("creting bucet if not exist", string(key))
	buc, ok := (*b)[string(key)]
	if !ok {
		buc = make(map[string]interface{})
		(*b)[string(key)] = buc
	}
	m, ok := buc.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("123")
	}
	return (*bucket)(&m), nil

}

// DeleteNestedBucket removes a nested bucket with the given key.  Returns
// ErrTxNotWritable if attempted against a read-only transaction and
// ErrBucketNotFound if the specified bucket does not exist.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) DeleteNestedBucket(key []byte) error {
	//fmt.Println("delete nested bucket", string(key))
	delete(*b, string(key))
	return nil
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This includes nested buckets, in which case the value is nil, but it does not
// include the key/value pairs within those nested buckets.
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	//fmt.Println("foreac")
	for k, v := range *b {
		key := []byte(k)
		val, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("faild 2334")
		}
		if err := fn(key, val); err != nil {
			return err
		}
	}
	return nil
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.  Returns
// ErrTxNotWritable if attempted against a read-only transaction.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	//fmt.Printf("putting (%x, %x)\n", key, value)
	//fmt.Println("map jas length", len((*b)))
	//for k, v := range *b {
	//fmt.Printf("k: %x, v:%v\n", k, v)
	//}
	(*b)[string(key)] = value
	//fmt.Println("after add has length", len((*b)))
	return nil
}

// Get returns the value for the given key.  Returns nil if the key does
// not exist in this bucket (or nested buckets).
//
// NOTE: The value returned by this function is only valid during a
// transaction.  Attempting to access it after a transaction has ended
// will likely result in an access violation.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	//fmt.Printf("getting key %x \n", key)
	//fmt.Println("map jas length", len((*b)))
	//	for k, v := range *b {
	//		fmt.Printf("k: %x, v:%v\n", k, v)
	//	}
	v, ok := (*b)[string(key)]
	if !ok {
		fmt.Println("did not find value")
		return nil
	}
	val, ok := v.([]byte)
	if !ok {

		fmt.Println("could not cast value")
		return nil
	}
	return val
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.  Returns ErrTxNotWritable if attempted
// against a read-only transaction.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	fmt.Println("delete key", string(key))
	delete(*b, string(key))
	return nil
}

func (b *bucket) ReadCursor() walletdb.ReadCursor {
	fmt.Println("read cursor")
	return b.ReadWriteCursor()
}

// ReadWriteCursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs and nested buckets in forward or backward order.
//
// This function is part of the walletdb.Bucket interface implementation.
func (b *bucket) ReadWriteCursor() walletdb.ReadWriteCursor {

	fmt.Println("read write cursor")
	c := &cursor{}
	return c
}

// cursor represents a cursor over key/value pairs and nested buckets of a
// bucket.
//
// Note that open cursors are not tracked on bucket changes and any
// modifications to the bucket, with the exception of cursor.Delete, invalidate
// the cursor. After invalidation, the cursor must be repositioned, or the keys
// and values returned may be unpredictable.
type cursor struct {
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor. Returns ErrTxNotWritable if attempted on a read-only
// transaction, or ErrIncompatibleValue if attempted when the cursor points to a
// nested bucket.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Delete() error {
	fmt.Println("delete cursor")
	panic("not implemented")
	return fmt.Errorf("delete not implemented")
}

// First positions the cursor at the first key/value pair and returns the pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) First() (key, value []byte) {

	fmt.Println("firstcursor")
	panic("first not implemented")
	return nil, nil
}

// Last positions the cursor at the last key/value pair and returns the pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Last() (key, value []byte) {
	panic("last not implemented")
	return nil, nil
}

// Next moves the cursor one key/value pair forward and returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Next() (key, value []byte) {
	fmt.Println("next cursor")
	panic("next not implemented")
	return nil, nil
}

// Prev moves the cursor one key/value pair backward and returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Prev() (key, value []byte) {
	fmt.Println("prev cursor")
	panic("prev not implemented")
	return nil, nil

}

// Seek positions the cursor at the passed seek key. If the key does not exist,
// the cursor is moved to the next key after seek. Returns the new pair.
//
// This function is part of the walletdb.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) (key, value []byte) {
	fmt.Println("seek cursor")
	panic("seek not implemented")
	return nil, nil

}

// db represents a collection of namespaces which are persisted and implements
// the walletdb.Db interface.  All database access is performed through
// transactions which are obtained through the specific Namespace.
type db map[string]interface{}

// Enforce db implements the walletdb.Db interface.
var _ walletdb.DB = (*db)(nil)

func (db *db) beginTx(writable bool) (*transaction, error) {
	//	fmt.Println("begin tx with map of lenght", len(*db))
	//	for k, v := range *db {
	//		fmt.Printf("k: %x, v:%v\n", k, v)
	//	}
	mtx.Lock()
	return &transaction{m: db}, nil
}

func (db *db) BeginReadTx() (walletdb.ReadTx, error) {
	return db.beginTx(false)
}

func (db *db) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	return db.beginTx(true)
}

// Copy writes a copy of the database to the provided writer.  This call will
// start a read-only transaction to perform all operations.
//
// This function is part of the walletdb.Db interface implementation.
func (db *db) Copy(w io.Writer) error {

	panic("not implemented")
	return fmt.Errorf("copy not implemented")
}

// Close cleanly shuts down the database and syncs all data.
//
// This function is part of the walletdb.Db interface implementation.
func (db *db) Close() error {
	panic("not implemented")
	return fmt.Errorf("close not implemented")
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// openDB opens the database at the provided path.  walletdb.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, create bool) (walletdb.DB, error) {
	database = make(map[string]interface{})
	fmt.Println("returning db:", database)
	return &database, nil
}
