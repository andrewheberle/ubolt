// Package ubolt wraps various calls from "go.etcd.io/bbolt" to make basic use simpler and quicker.
//
// Various calls such as Get, Put etc are automatically wrapped in transactions to ensure consistency.
package ubolt

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Bucket struct {
	db     *Database
	bucket []byte
}

// ErrBucketNotFound is returned when the bucket requested was not found.
type ErrBucketNotFound struct {
	bucket []byte
}

// Error returns the formatted configuration error.
func (e ErrBucketNotFound) Error() string {
	return fmt.Sprintf("bucket %s not found", string(e.bucket))
}

// Is allows testing using errors.Is
func (e ErrBucketNotFound) Is(target error) bool {
	_, is := target.(ErrBucketNotFound)

	return is
}

// ErrKeyNotFound is returned when the key requested was not found
type ErrKeyNotFound struct {
	bucket []byte
	key    []byte
}

// Error returns the formatted configuration error.
func (e ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %s not found in bucket %s", string(e.key), string(e.bucket))
}

// Is allows testing using errors.Is
func (e ErrKeyNotFound) Is(target error) bool {
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

// OpenBucket performs the same process as Open however all operations are performed on the specified bucket
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

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
