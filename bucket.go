package ubolt

import bolt "go.etcd.io/bbolt"

type Bucket struct {
	db     *Database
	bucket []byte
}

// BoltDB provides access to the underlying bbolt.DB if lower level access is required
func (b *Bucket) BoltDB() *bolt.DB {
	return b.db.BoltDB()
}

// Close releases all database resources and closes the file. This call will block while any open transactions complete.
func (b *Bucket) Close() error {
	return b.db.Close()
}

// Ping tests the database by attempting to retrieve a list of buckets.
func (b *Bucket) Ping() error {
	return b.db.Ping()
}

// Put sets the specified key in the bucket opened to the provided value. This process is wrapped in a read/write transaction.
func (b *Bucket) Put(key, value []byte) error {
	return b.db.Put(b.bucket, key, value)
}

// PutV sets a key based on an auto-incrementing value for the key.
func (b *Bucket) PutV(value []byte) (key []byte, err error) {
	return b.db.PutV(b.bucket, value)
}

// GetE retrieves the specified key and returns the value and an error. The returned error is non-nil if a failure occurred, which includes if the key was not found.
func (b *Bucket) GetE(key []byte) (value []byte, err error) {
	return b.db.GetE(b.bucket, key)
}

// Get retrieves the specified key and returns the value. The value returned may be nil which indicates the key was not found.
func (b *Bucket) Get(key []byte) (value []byte) {
	return b.db.Get(b.bucket, key)
}

// Encode encodes the provided value using "encoding/gob" then writes the resulting byte slice to the provided key
func (b *Bucket) Encode(key []byte, value any) error {
	return b.db.Encode(b.bucket, key, value)
}

// Decode retrieves and decodes a value set by Encode into the provided pointer value.
func (b *Bucket) Decode(key []byte, value any) error {
	return b.db.Decode(b.bucket, key, value)
}

// Delete removes the specified key. This process is wrapped in a read/write transaction.
func (b *Bucket) Delete(key []byte) error {
	return b.db.Delete(b.bucket, key)
}

func (b *Bucket) GetKeysE() (keys [][]byte, err error) {
	return b.db.GetKeysE(b.bucket)
}

func (b *Bucket) GetKeys() (keys [][]byte) {
	return b.db.GetKeys(b.bucket)
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	return b.db.ForEach(b.bucket, fn)
}

func (b *Bucket) Scan(prefix []byte, fn func(k, v []byte) error) error {
	return b.db.Scan(b.bucket, prefix, fn)
}
