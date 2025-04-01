package ubolt

import (
	"bytes"
	"encoding/gob"
	"io"

	bolt "go.etcd.io/bbolt"
)

type Database struct {
	db *bolt.DB
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (db *Database) Close() error {
	return db.db.Close()
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (db *Database) Ping() error {
	_, err := db.GetBucketsE()
	return err
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

// Get retrieves the specified key from the chosen bucket and returns the value. The value returned may be nil which indicates the bucket or key was not found.
func (db *Database) Get(bucket, key []byte) (value []byte) {
	value, _ = db.GetE(bucket, key)

	return value
}

// Encode encodes the provided value using "encoding/gob" then writes the resulting byte slice to the provided key
func (db *Database) Encode(bucket, key []byte, value any) error {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return err
	}

	return db.Put(bucket, key, buf.Bytes())
}

// Decode retrieves and decodes a value set by Encode into the provided pointer value.
func (db *Database) Decode(bucket, key []byte, value any) error {
	data, err := db.GetE(bucket, key)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	return dec.Decode(value)
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

func (db *Database) GetKeys(bucket []byte) (keys [][]byte) {
	keys, _ = db.GetKeysE(bucket)

	return keys
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
