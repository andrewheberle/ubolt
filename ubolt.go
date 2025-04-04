// Package ubolt wraps various calls from "go.etcd.io/bbolt" to make basic use simpler and quicker.
//
// Various calls such as Get, Put etc are automatically wrapped in transactions to ensure consistency.
package ubolt

import (
	"encoding/binary"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	DefaultTimeout = 5 * time.Second
	DefaultMode    = 0600
)

// Open creates and opens a database at the given path. If the file does not exist it will be created automatically
// with a file-mode of DefaultMode, or the provided WithMode Option.
//
// The default timeout to obtain a lock on the database is based on DefaultTimeout, howevber this can be
// overridden by passing the WithTimeout Option to Open.
func Open(path string, opts ...Option) (*Database, error) {
	d := new(Database)

	// defaults
	d.timeout = DefaultTimeout
	d.mode = DefaultMode

	// apply options
	for _, o := range opts {
		o(d)
	}

	// open database
	db, err := bolt.Open(path, d.mode, &bolt.Options{Timeout: d.timeout, OpenFile: d.openFile})
	if err != nil {
		return nil, err
	}

	d.db = db

	return d, nil
}

// OpenBucket performs the same process as Open however all subsequent operations, such as Get and Put are performed on the specified bucket
func OpenBucket(path string, bucket []byte, opts ...Option) (*Bucket, error) {
	db, err := Open(path, opts...)
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
