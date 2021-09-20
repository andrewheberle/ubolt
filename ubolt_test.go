package ubolt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testdb     = "test.db"
	testbucket = []byte("bucket1")
	onebucket  = [][]byte{[]byte("bucket1")}
	twobuckets = [][]byte{[]byte("bucket1"), []byte("bucket2")}
)

func TestOpen(t *testing.T) {
	_ = os.Remove(testdb)

	// test no buckets
	func() {
		defer os.Remove(testdb)

		db, err := Open(testdb, nil)
		assert.Nil(t, err)
		defer db.Close()
	}()

	// test one bucket
	func() {
		defer os.Remove(testdb)

		db, err := Open(testdb, onebucket)
		assert.Nil(t, err)
		defer db.Close()
	}()

	// test two buckets
	func() {
		defer os.Remove(testdb)

		db, err := Open(testdb, twobuckets)
		assert.Nil(t, err)
		defer db.Close()
	}()
}

func TestPut(t *testing.T) {
	defer os.Remove(testdb)

	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"simple put", testbucket, []byte("key1"), []byte("value"), false},
		{"missing bucket", []byte("missing"), []byte("key1"), []byte("value"), true},
	}

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := Open(testdb, [][]byte{testbucket})
	assert.Nil(t, err)
	defer db.Close()

	for _, tt := range tests {
		err := db.Put(tt.bucket, tt.key, tt.value)
		if tt.wantErr {
			assert.NotNil(t, err, tt.name)
		} else {
			assert.Nil(t, err, tt.name)
		}
	}
}

func TestGet(t *testing.T) {
	defer os.Remove(testdb)

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := Open(testdb, [][]byte{testbucket})
	assert.Nil(t, err)
	defer db.Close()

	// test no data
	func() {
		data := db.Get(testbucket, []byte("key"))
		assert.Nil(t, data, "missing key or bucket")
	}()

	// add some data
	func() {
		err := db.Put(testbucket, []byte("key"), []byte("value"))
		assert.Nil(t, err)
	}()

	// get data
	func() {
		value := db.Get(testbucket, []byte("key"))
		assert.Equal(t, []byte("value"), value)
	}()
}

func TestGetE(t *testing.T) {
	defer os.Remove(testdb)

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := Open(testdb, [][]byte{testbucket})
	assert.Nil(t, err)
	defer db.Close()

	// test missing bucket
	func() {
		data, err := db.GetE([]byte("missingbucket"), []byte("key"))
		assert.Nil(t, data, "missing bucket")
		assert.ErrorIs(t, err, ErrorBucketNotFound, "missing bucket")
	}()

	// test missing key
	func() {
		data, err := db.GetE(testbucket, []byte("key"))
		assert.Nil(t, data, "missing key")
		assert.ErrorIs(t, err, ErrorKeyNotFound, "missing key")
	}()

	// add some data
	func() {
		err := db.Put(testbucket, []byte("key"), []byte("value"))
		assert.Nil(t, err)
	}()

	// get data
	func() {
		value, err := db.GetE(testbucket, []byte("key"))
		assert.Equal(t, []byte("value"), value)
		assert.Nil(t, err)
	}()
}

func TestGetKeys(t *testing.T) {
	defer os.Remove(testdb)

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := Open(testdb, [][]byte{testbucket})
	assert.Nil(t, err)
	defer db.Close()

	// test no keys in bucket
	func() {
		keys := db.GetKeys(testbucket)
		assert.Nil(t, keys, "empty bucket")
	}()

	// test with one keys
	func() {
		_ = db.Put(testbucket, []byte("key"), []byte("value"))
		keys := db.GetKeys(testbucket)
		assert.Equal(t, [][]byte{[]byte("key")}, keys, "has one key")
	}()

	// test with many keys
	func() {
		keylist := [][]byte{
			[]byte("key"),
			[]byte("key1"),
			[]byte("key2"),
			[]byte("key3"),
		}
		for _, k := range keylist {
			_ = db.Put(testbucket, k, []byte("value"))
		}

		keys := db.GetKeys(testbucket)
		assert.Equal(t, keylist, keys, "many keys")
	}()
}
