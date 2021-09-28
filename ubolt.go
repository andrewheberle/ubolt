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

type DB struct {
	db *bolt.DB
}

type BDB struct {
	db     *DB
	bucket []byte
}

// ErrBucketNotFound is returned when the bucket requested was not found
type ErrBucketNotFound struct {
	bucket []byte
}

func (bnf ErrBucketNotFound) Error() string {
	return fmt.Sprintf("Bucket %s not found", string(bnf.bucket))
}

func (bnf ErrBucketNotFound) Is(target error) bool {
	_, is := target.(ErrBucketNotFound)

	return is
}

// ErrKeyNotFound is returned when the key requested was not found
type ErrKeyNotFound struct {
	bucket []byte
	key    []byte
}

func (knf ErrKeyNotFound) Error() string {
	return fmt.Sprintf("Key %s not found in bucket %s", string(knf.key), string(knf.bucket))
}

func (knf ErrKeyNotFound) Is(target error) bool {
	_, is := target.(ErrKeyNotFound)

	return is
}

// Open creates and opens a database at the given path. If the file does not exist it will be created automatically.
// The database is opened with a file-mode of 0600 and a timeout of 5 seconds
func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

// OpenBucket performs the same process as Open however only one bucket is usable in subsequent calls to Put, Get etc
func OpenBucket(path string, bucket []byte) (*BDB, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}

	if err := db.CreateBucket(bucket); err != nil {
		return nil, err
	}

	return &BDB{db: db, bucket: bucket}, nil
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (db *DB) Close() error {
	return db.db.Close()
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (bdb *BDB) Close() error {
	return bdb.db.Close()
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (db *DB) Ping() error {
	_, err := db.GetBucketsE()
	return err
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (bdb *BDB) Ping() error {
	return bdb.db.Ping()
}

// Put sets the specified key in the chosen bucket to the provided value. This process is wrapped in a read/write transaction.
func (db *DB) Put(bucket, key, value []byte) error {
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

func (bdb *BDB) Put(key, value []byte) error {
	return bdb.db.Put(bdb.bucket, key, value)
}

// PutV sets a key based on an auto-incrementing value for the key
func (db *DB) PutV(bucket, value []byte) (key []byte, err error) {
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

func (bdb *BDB) PutV(value []byte) (key []byte, err error) {
	return bdb.db.PutV(bdb.bucket, value)
}

func (db *DB) GetE(bucket, key []byte) (value []byte, err error) {
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

func (bdb *BDB) GetE(key []byte) (value []byte, err error) {
	return bdb.db.GetE(bdb.bucket, key)
}

func (db *DB) Get(bucket, key []byte) (value []byte) {
	value, _ = db.GetE(bucket, key)

	return value
}

func (bdb *BDB) Get(key []byte) (value []byte) {
	return bdb.db.Get(bdb.bucket, key)
}

// Encode encodes the provided value using "encoding/gob" then writes the resulting byte slice to the provided key
func (db *DB) Encode(bucket, key []byte, value interface{}) error {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return err
	}

	return db.Put(bucket, key, buf.Bytes())
}

func (bdb *BDB) Encode(key []byte, value interface{}) error {
	return bdb.db.Encode(bdb.bucket, key, value)
}

func (db *DB) Decode(bucket, key []byte, value interface{}) error {
	data, err := db.GetE(bucket, key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	return dec.Decode(value)
}

func (bdb *BDB) Decode(key []byte, value interface{}) error {
	return bdb.db.Decode(bdb.bucket, key, value)
}

// Delete removes the specified key in the chosen bucket. This process is wrapped in a read/write transaction.
func (db *DB) Delete(bucket, key []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		return b.Delete(key)
	})
}

func (bdb *BDB) Delete(key []byte) error {
	return bdb.db.Delete(bdb.bucket, key)
}

// DeleteBucket removes the specified bucket. This also deletes all keys contained in the bucket and any nested buckets.
func (db *DB) DeleteBucket(bucket []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucket)
	})
}

// DeleteBucket removes the specified bucket. This also deletes all keys contained in the bucket and any nested buckets.
func (db *DB) CreateBucket(bucket []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)

		return err
	})
}

func (db *DB) GetKeysE(bucket []byte) (keys [][]byte, err error) {
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

func (bdb *BDB) GetKeysE() (keys [][]byte, err error) {
	return bdb.db.GetKeysE(bdb.bucket)
}

func (db *DB) GetKeys(bucket []byte) (keys [][]byte) {
	keys, _ = db.GetKeysE(bucket)

	return keys
}

func (bdb *BDB) GetKeys() (keys [][]byte) {
	return bdb.db.GetKeys(bdb.bucket)
}

func (db *DB) GetBucketsE() (buckets [][]byte, err error) {
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

func (db *DB) GetBuckets() (buckets [][]byte) {
	buckets, _ = db.GetBucketsE()

	return buckets
}

func (db *DB) ForEach(bucket []byte, fn func(k, v []byte) error) error {
	return db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		if b == nil {
			return ErrBucketNotFound{bucket}
		}

		return b.ForEach(fn)
	})
}

func (bdb *BDB) ForEach(fn func(k, v []byte) error) error {
	return bdb.db.ForEach(bdb.bucket, fn)
}

func (db *DB) Scan(bucket, prefix []byte, fn func(k, v []byte) error) error {
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

func (bdb *BDB) Scan(prefix []byte, fn func(k, v []byte) error) error {
	return bdb.db.Scan(bdb.bucket, prefix, fn)
}

func (db *DB) WriteTo(w io.Writer) (n int64, err error) {
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
