package ubolt

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	testdb     = "test.db"
	testbackup = "test.bak"
	testbucket = []byte("bucket1")
	testkey    = []byte("key1")
	testvalue  = []byte("value1")
	missing    = []byte("missing")
)

type UboltDBTestSuite struct {
	suite.Suite
	db     *DB
	bdb    *BDB
	Bucket bool
}

func (s *UboltDBTestSuite) SetupTest() {
	// start with no database
	_ = os.Remove(testdb)

	if s.Bucket {
		// set up db
		db, err := OpenBucket(testdb, testbucket)
		if err != nil {
			panic(err)
		}

		if err := db.Put(testkey, testvalue); err != nil {
			panic(err)
		}

		s.bdb = db
	} else {
		// set up db
		db, err := Open(testdb)
		if err != nil {
			panic(err)
		}
		if err := db.CreateBucket(testbucket); err != nil {
			panic(err)
		}

		if err := db.Put(testbucket, testkey, testvalue); err != nil {
			panic(err)
		}

		s.db = db
	}
}

func (s *UboltDBTestSuite) TestPut() {
	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"Put - missing bucket (no key)", missing, nil, nil, true},
		{"Put - missing bucket", missing, testkey, nil, true},
		{"Put - valid bucket", testbucket, testkey, testvalue, false},
	}

	for _, tt := range tests {
		var err error

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		if s.Bucket {
			err = s.bdb.Put(tt.key, tt.value)
		} else {
			err = s.db.Put(tt.bucket, tt.key, tt.key)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
		}
	}
}

func (s *UboltDBTestSuite) TestPutV() {
	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"PutV - missing bucket", missing, nil, nil, true},
		{"PutV - valid bucket - 1", testbucket, itob(1), testvalue, false},
		{"PutV - valid bucket - 2", testbucket, itob(2), testvalue, false},
	}

	for _, tt := range tests {
		var key []byte
		var err error

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		if s.Bucket {
			key, err = s.bdb.PutV(tt.value)
		} else {
			key, err = s.db.PutV(tt.bucket, tt.key)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
			assert.Equal(s.T(), tt.key, key, tt.name)
		}
	}
}

func (s *UboltDBTestSuite) TestGet() {
	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"Get - missing bucket", missing, []byte("key2"), nil, true},
		{"Get - missing key", testbucket, missing, nil, true},
		{"Get - valid key", testbucket, testkey, testvalue, false},
	}

	for _, tt := range tests {
		var got []byte

		if s.Bucket {
			got = s.bdb.Get(tt.key)
		} else {
			got = s.db.Get(tt.bucket, tt.key)
		}

		if tt.wantErr {
			assert.Nil(s.T(), got, tt.name)
		} else {
			assert.NotNil(s.T(), got, tt.name)
			assert.Equal(s.T(), tt.value, got, tt.name)
		}
	}
}

func (s *UboltDBTestSuite) TestGetE() {
	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"GetE - missing bucket", missing, []byte("key2"), nil, true},
		{"GetE - missing key", testbucket, missing, nil, true},
		{"GetE - valid key", testbucket, testkey, testvalue, false},
	}

	for _, tt := range tests {
		var got []byte
		var err error

		if s.Bucket {
			got, err = s.bdb.GetE(tt.key)
		} else {
			got, err = s.db.GetE(tt.bucket, tt.key)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
			assert.Equal(s.T(), tt.value, got, tt.name)
		}
	}

}

func (s *UboltDBTestSuite) TestGetKeys() {
	tests := []struct {
		name    string
		bucket  []byte
		keys    [][]byte
		wantErr bool
	}{
		{"GetKeys - missing bucket", missing, [][]byte{[]byte("key2")}, true},
		{"GetKeys - valid bucket", testbucket, [][]byte{testkey}, false},
	}

	// run tests
	for _, tt := range tests {
		var keys [][]byte

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		if s.Bucket {
			keys = s.bdb.GetKeys()
		} else {
			keys = s.db.GetKeys(tt.bucket)
		}

		if tt.wantErr {
			assert.Nil(s.T(), keys, tt.name)
		} else {
			assert.Equal(s.T(), tt.keys, keys, tt.name)
		}
	}

}

func (s *UboltDBTestSuite) TestGetKeysE() {
	tests := []struct {
		name    string
		bucket  []byte
		keys    [][]byte
		wantErr bool
	}{
		{"GetKeysE - missing bucket", missing, [][]byte{[]byte("key2")}, true},
		{"GetKeysE - valid bucket", testbucket, [][]byte{testkey}, false},
	}

	// run tests
	for _, tt := range tests {
		var keys [][]byte
		var err error

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		if s.Bucket {
			keys, err = s.bdb.GetKeysE()
		} else {
			keys, err = s.db.GetKeysE(tt.bucket)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
			assert.Equal(s.T(), tt.keys, keys, tt.name)
		}
	}

}

func (s *UboltDBTestSuite) TestGetBuckets() {
	if s.Bucket {
		return
	}

	got := s.db.GetBuckets()
	assert.Equal(s.T(), [][]byte{testbucket}, got, "GetBuckets")
}

func (s *UboltDBTestSuite) TestGetBucketsE() {
	if s.Bucket {
		return
	}

	got, err := s.db.GetBucketsE()
	assert.Nil(s.T(), err, "GetBucketsE")
	assert.Equal(s.T(), [][]byte{testbucket}, got, "GetBucketsE")
}

func (s *UboltDBTestSuite) TestDelete() {
	tests := []struct {
		name    string
		bucket  []byte
		key     []byte
		wantErr bool
	}{
		{"Delete - missing bucket", missing, []byte("key2"), true},
		{"Delete - missing key", testbucket, missing, false},
		{"Delete - valid key", testbucket, testkey, false},
	}

	for _, tt := range tests {
		var err error

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		if s.Bucket {
			err = s.bdb.Delete(tt.key)
		} else {
			err = s.db.Delete(tt.bucket, tt.key)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
		}
	}

}

func (s *UboltDBTestSuite) TestDeleteBucket() {
	if s.Bucket {
		return
	}

	tests := []struct {
		name    string
		bucket  []byte
		wantErr bool
	}{
		{"DeleteBucket - missing bucket", missing, true},
		{"DeleteBucket - valid bucket", testbucket, false},
	}

	for _, tt := range tests {
		err := s.db.DeleteBucket(tt.bucket)

		if tt.wantErr {
			assert.NotNil(s.T(), err, tt.name)
		} else {
			assert.Nil(s.T(), err, tt.name)
		}
	}

}

func (s *UboltDBTestSuite) TestScan() {
	// run Scan tets
	got := make([]string, 0)
	scantests := []struct {
		name    string
		bucket  []byte
		prefix  []byte
		fn      func(k, v []byte) error
		want    string
		wantErr bool
	}{
		{
			name:   "basic",
			bucket: testbucket,
			prefix: []byte("key"),
			fn: func(k, v []byte) error {
				got = append(got, fmt.Sprintf("k=%s;v=%s", k, v))
				return nil
			},
			want:    "k=key1;v=value1",
			wantErr: false,
		},
		{
			name:   "missing bucket",
			bucket: missing,
			prefix: []byte("key"),
			fn: func(k, v []byte) error {
				return nil
			},
			wantErr: true,
		},
		{
			name:   "missing prefix",
			bucket: testbucket,
			prefix: missing,
			fn: func(k, v []byte) error {
				got = append(got, fmt.Sprintf("k=%s;v=%s", k, v))
				return nil
			},
			want:    "",
			wantErr: false,
		},
	}

	for _, tt := range scantests {
		var err error

		// skip test if this is a bucket only test looking for a missing bucket
		if s.Bucket && bytes.Equal(tt.bucket, missing) {
			continue
		}

		got = make([]string, 0)

		if s.Bucket {
			err = s.bdb.Scan(tt.prefix, tt.fn)
		} else {
			err = s.db.Scan(tt.bucket, tt.prefix, tt.fn)
		}

		if tt.wantErr {
			assert.NotNil(s.T(), err)
		} else {
			assert.Nil(s.T(), err)
			assert.Equal(s.T(), tt.want, strings.Join(got, ":"), tt.name)
		}
	}
}

func (s *UboltDBTestSuite) TestEncode() {
	// run Encode/Decode tests
	type enctest struct {
		Name   string
		Number int
	}
	encodetests := []struct {
		name    string
		bucket  []byte
		key     []byte
		value   interface{}
		wantErr bool
	}{
		{"string", testbucket, []byte("string"), "string", false},
		{"int", testbucket, []byte("int"), 100, false},
		{"struct", testbucket, []byte("struct"), enctest{"name", 100}, false},
	}

	for _, tt := range encodetests {
		var err error

		switch tt.value.(type) {
		case string:
			// start with non-zero value
			var got = string("somevalue")

			if s.Bucket {
				err = s.bdb.Encode(tt.key, tt.value.(string))
			} else {
				err = s.db.Encode(tt.bucket, tt.key, tt.value.(string))
			}
			if tt.wantErr {
				assert.NotNil(s.T(), err, tt.name)
			} else {
				assert.Nil(s.T(), err, tt.name)
				if s.Bucket {
					err = s.bdb.Decode(tt.key, &got)
				} else {
					err = s.db.Decode(tt.bucket, tt.key, &got)
				}
				if tt.wantErr {
					assert.NotNil(s.T(), err, tt.name)
				} else {
					assert.Nil(s.T(), err, tt.name)
					assert.Equal(s.T(), tt.value.(string), got, tt.name)
				}
			}
		case int:
			// start with non-zero value
			var got = int(1000)

			if s.Bucket {
				err = s.bdb.Encode(tt.key, tt.value.(int))
			} else {
				err = s.db.Encode(tt.bucket, tt.key, tt.value.(int))
			}
			if tt.wantErr {
				assert.NotNil(s.T(), err, tt.name)
			} else {
				assert.Nil(s.T(), err, tt.name)
				if s.Bucket {
					err = s.bdb.Decode(tt.key, &got)
				} else {
					err = s.db.Decode(tt.bucket, tt.key, &got)
				}
				if tt.wantErr {
					assert.NotNil(s.T(), err, tt.name)
				} else {
					assert.Nil(s.T(), err, tt.name)
					assert.Equal(s.T(), tt.value.(int), got, tt.name)
				}
			}
		case enctest:
			// start with non-zero value
			var got = enctest{"somename", 100}

			if s.Bucket {
				err = s.bdb.Encode(tt.key, tt.value.(enctest))
			} else {
				err = s.db.Encode(tt.bucket, tt.key, tt.value.(enctest))
			}
			if tt.wantErr {
				assert.NotNil(s.T(), err, tt.name)
			} else {
				assert.Nil(s.T(), err, tt.name)
				if s.Bucket {
					err = s.bdb.Decode(tt.key, &got)
				} else {
					err = s.db.Decode(tt.bucket, tt.key, &got)
				}
				if tt.wantErr {
					assert.NotNil(s.T(), err, tt.name)
				} else {
					assert.Nil(s.T(), err, tt.name)
					assert.Equal(s.T(), tt.value.(enctest), got, tt.name)
				}
			}
		}
		if tt.wantErr {
			assert.NotNil(s.T(), err, "Encode", tt.name)
		} else {
			assert.Nil(s.T(), err, "Encode", tt.name)
		}
	}
}

func (s *UboltDBTestSuite) TestPing() {
	var err error

	if s.Bucket {
		err = s.bdb.Ping()
	} else {
		err = s.db.Ping()
	}

	assert.Nil(s.T(), err, "Ping")
}

func (s *UboltDBTestSuite) TestWriteTo() {
	if s.Bucket {
		return
	}
	defer os.Remove(testbackup)

	n, err := func() (int64, error) {
		f, err := os.Create(testbackup)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		return s.db.WriteTo(f)
	}()

	assert.Nil(s.T(), err, "WriteTo")

	info, err := os.Stat(testbackup)
	if err != nil {
		panic(err)
	}

	assert.Equal(s.T(), info.Size(), n, "WriteTo - Size check")

	// check data was found in backup
	db, err := OpenBucket(testbackup, testbucket)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	key, err := db.GetE(testkey)
	if err != nil {
		panic(err)
	}

	assert.Equal(s.T(), testvalue, key, "WriteTo - Read from backup")
}

func (s *UboltDBTestSuite) TearDownTest() {
	if s.Bucket {
		if err := s.bdb.Close(); err != nil {
			panic(err)
		}
	} else {
		if err := s.db.Close(); err != nil {
			panic(err)
		}
	}

	_ = os.Remove(testdb)
}

func TestUboltDBTestSuite(t *testing.T) {
	suite.Run(t, new(UboltDBTestSuite))
	suite.Run(t, &UboltDBTestSuite{Bucket: true})
}
