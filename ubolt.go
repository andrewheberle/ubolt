// Package ubolt wraps various calls from "go.etcd.io/bbolt" to make basic use simpler and quicker.
//
// Various calls such as Get, Put etc are automatically wrapped in transactions to ensure consistency.
package ubolt

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Database struct {
	db *bolt.DB
}

type Bucket struct {
	db     *Database
	bucket []byte
}

// ErrBucketNotFound is returned when the bucket requested was not found.
type ErrBucketNotFound struct {
	bucket []byte
}

// Error returns the formatted configuration error.
func (bnf ErrBucketNotFound) Error() string {
	return fmt.Sprintf("Bucket %s not found", string(bnf.bucket))
}

// Is allows testing using errors.Is
func (bnf ErrBucketNotFound) Is(target error) bool {
	_, is := target.(ErrBucketNotFound)

	return is
}

// ErrKeyNotFound is returned when the key requested was not found
type ErrKeyNotFound struct {
	bucket []byte
	key    []byte
}

// Error returns the formatted configuration error.
func (knf ErrKeyNotFound) Error() string {
	return fmt.Sprintf("Key %s not found in bucket %s", string(knf.key), string(knf.bucket))
}

// Is allows testing using errors.Is
func (knf ErrKeyNotFound) Is(target error) bool {
	_, is := target.(ErrKeyNotFound)

	return is
}

// Open creates and opens a database at the given path. If the file does not exist it will be created automatically.
// The database is opened with a file-mode of 0600 and a timeout of 5 seconds
func Open(path string) (*Database, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}

	return &Database{db}, nil
}

// OpenBucket performs the same process as Open however only one bucket is usable in subsequent calls to Put, Get etc
func OpenBucket(path string, bucket []byte) (*Bucket, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}

	if err := db.CreateBucket(bucket); err != nil {
		return nil, err
	}

	return &Bucket{db: db, bucket: bucket}, nil
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (db *Database) Close() error {
	return db.db.Close()
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (b *Bucket) Close() error {
	return b.db.Close()
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (db *Database) Ping() error {
	_, err := db.GetBucketsE()
	return err
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (b *Bucket) Ping() error {
	return b.db.Ping()
}

// Put sets the specified key in the chosen bucket to the provided value. This process is wrapped in a read/write transaction.
func (db *Database) Put(bucket, key, value []byte) error {
	if key == nil {
		_, err := db.PutV(bucket, value)

		return err
	}

	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		return b.Put(key, value)
	})
}

// Put sets the specified key in the bucket opened to the provided value. This process is wrapped in a read/write transaction.
func (b *Bucket) Put(key, value []byte) error {
	return b.db.Put(b.bucket, key, value)
}

// PutV sets a key based on an auto-incrementing value for the key.
func (db *Database) PutV(bucket, value []byte) (key []byte, err error) {
	err = db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		// generate key
		id, err := b.NextSequence()
		if err != nil {
			return err
		}

		// convert id into []byte
		key = itob(id)

		return b.Put(key, value)
	})

	if err != nil {
		return nil, err
	}

	return key, nil
}

// PutV sets a key based on an auto-incrementing value for the key.
func (b *Bucket) PutV(value []byte) (key []byte, err error) {
	return b.db.PutV(b.bucket, value)
}

// GetE retrieves the specified key from the chosen bucket and returns the value and an error. The returned error is non-nil if a failure occurred, which includes if the bucket or key was not found.
func (db *Database) GetE(bucket, key []byte) (value []byte, err error) {
	if err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		data := b.Get(key)
		if data == nil {
			return ErrKeyNotFound{bucket: bucket, key: key}
		}

		value = append(value, data...)

		return nil
	}); err != nil {
		return nil, err
	}

	return value, nil
}

// GetE retrieves the specified key and returns the value and an error. The returned error is non-nil if a failure occurred, which includes if the key was not found.
func (b *Bucket) GetE(key []byte) (value []byte, err error) {
	return b.db.GetE(b.bucket, key)
}

// Get retrieves the specified key from the chosen bucket and returns the value. The value returned may be nil which indicates the bucket or key was not found.
func (db *Database) Get(bucket, key []byte) (value []byte) {
	value, _ = db.GetE(bucket, key)

	return value
}

// Get retrieves the specified key and returns the value. The value returned may be nil which indicates the key was not found.
func (b *Bucket) Get(key []byte) (value []byte) {
	return b.db.Get(b.bucket, key)
}

// Encode encodes the provided value using "encoding/gob" then writes the resulting byte slice to the provided key
func (db *Database) Encode(bucket, key []byte, value interface{}) error {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return err
	}

	return db.Put(bucket, key, buf.Bytes())
}

// Encode encodes the provided value using "encoding/gob" then writes the resulting byte slice to the provided key
func (b *Bucket) Encode(key []byte, value interface{}) error {
	return b.db.Encode(b.bucket, key, value)
}

// Decode retrieves and decodes a value set by Encode into the provided pointer value.
func (db *Database) Decode(bucket, key []byte, value interface{}) error {
	data, err := db.GetE(bucket, key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	return dec.Decode(value)
}

// Decode retrieves and decodes a value set by Encode into the provided pointer value.
func (b *Bucket) Decode(key []byte, value interface{}) error {
	return b.db.Decode(b.bucket, key, value)
}

// Delete removes the specified key in the chosen bucket. This process is wrapped in a read/write transaction.
func (db *Database) Delete(bucket, key []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		return b.Delete(key)
	})
}

// Delete removes the specified key. This process is wrapped in a read/write transaction.
func (b *Bucket) Delete(key []byte) error {
	return b.db.Delete(b.bucket, key)
}

// DeleteBucket removes the specified bucket. This also deletes all keys contained in the bucket and any nested buckets.
func (db *Database) DeleteBucket(bucket []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucket)
	})
}

// DeleteBucket removes the specified bucket. This also deletes all keys contained in the bucket and any nested buckets.
func (db *Database) CreateBucket(bucket []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)

		return err
	})
}

func (db *Database) GetKeysE(bucket []byte) (keys [][]byte, err error) {
	if err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, k)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return keys, nil
}

func (b *Bucket) GetKeysE() (keys [][]byte, err error) {
	return b.db.GetKeysE(b.bucket)
}

func (db *Database) GetKeys(bucket []byte) (keys [][]byte) {
	keys, _ = db.GetKeysE(bucket)

	return keys
}

func (b *Bucket) GetKeys() (keys [][]byte) {
	return b.db.GetKeys(b.bucket)
}

func (db *Database) GetBucketsE() (buckets [][]byte, err error) {
	if err := db.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			buckets = append(buckets, name)
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return buckets, nil
}

func (db *Database) GetBuckets() (buckets [][]byte) {
	buckets, _ = db.GetBucketsE()

	return buckets
}

func (db *Database) ForEach(bucket []byte, fn func(k, v []byte) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		return b.ForEach(fn)
	})
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	return b.db.ForEach(b.bucket, fn)
}

func (db *Database) Scan(bucket, prefix []byte, fn func(k, v []byte) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		c := b.Cursor()

		for key, val := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, val = c.Next() {
			if err := fn(key, val); err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *Bucket) Scan(prefix []byte, fn func(k, v []byte) error) error {
	return b.db.Scan(b.bucket, prefix, fn)
}

func (db *Database) WriteTo(w io.Writer) (n int64, err error) {
	if err := db.db.View(func(tx *bolt.Tx) error {
		var err error

		n, err = tx.WriteTo(w)

		return err
	}); err != nil {
		return n, err
	}

	return n, nil
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
