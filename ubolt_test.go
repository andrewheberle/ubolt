package ubolt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testdb     = "test.db"
	testbucket = []byte("bucket1")
)

func TestDB(t *testing.T) {
	tests := map[string][]struct {
		name    string
		buckets [][]byte
		keys    [][]byte
		value   []byte
		wantErr bool
	}{
		"Put": {
			{"simple put key1", [][]byte{testbucket}, [][]byte{[]byte("key1")}, []byte("value1"), false},
			{"simple put key2", [][]byte{testbucket}, [][]byte{[]byte("key2")}, []byte("value2"), false},
			{"auto-increment key", [][]byte{testbucket}, nil, []byte("value2"), false},
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, []byte("value2"), true},
		},
		"PutV": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, []byte("value2"), true},
		},
		"Get": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, nil, true},
			{"missing key", [][]byte{testbucket}, [][]byte{[]byte("missing")}, nil, true},
			{"valid key", [][]byte{testbucket}, [][]byte{[]byte("key1")}, []byte("value1"), false},
		},
		"GetE": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, nil, true},
			{"missing key", [][]byte{testbucket}, [][]byte{[]byte("missing")}, nil, true},
			{"valid key", [][]byte{testbucket}, [][]byte{[]byte("key1")}, []byte("value1"), false},
		},
		"Delete": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, nil, true},
			{"missing key", [][]byte{testbucket}, [][]byte{[]byte("missing")}, nil, false},
			{"valid key", [][]byte{testbucket}, [][]byte{[]byte("key2")}, nil, false},
		},
		"GetKeys": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, nil, true},
			{"valid bucket", [][]byte{testbucket}, [][]byte{itob(uint64(1)), []byte("key1")}, nil, false},
		},
		"GetKeysE": {
			{"missing bucket", [][]byte{[]byte("missing")}, [][]byte{[]byte("key2")}, nil, true},
			{"valid bucket", [][]byte{testbucket}, [][]byte{itob(uint64(1)), []byte("key1")}, nil, false},
		},
		"GetBuckets": {
			{"valid bucket", [][]byte{testbucket}, nil, nil, false},
		},
		"DeleteBucket": {
			{"missing bucket", [][]byte{[]byte("missing")}, nil, nil, true},
			{"valid bucket", [][]byte{testbucket}, nil, nil, false},
		},
	}

	defer os.Remove(testdb)

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := Open(testdb)
	assert.Nil(t, err)
	defer db.Close()

	assert.Nil(t, db.CreateBucket(testbucket))

	// run Put tests
	for _, tt := range tests["Put"] {
		var err error
		if tt.keys == nil {
			err = db.Put(tt.buckets[0], nil, tt.value)
		} else {
			err = db.Put(tt.buckets[0], tt.keys[0], tt.value)
		}
		if tt.wantErr {
			assert.NotNil(t, err, "Put", tt.name)
		} else {
			assert.Nil(t, err, "Put", tt.name)
		}
	}

	// run PutV tests
	for _, tt := range tests["PutV"] {
		_, err := db.PutV(tt.buckets[0], tt.value)
		if tt.wantErr {
			assert.NotNil(t, err, "PutV", tt.name)
		} else {
			assert.Nil(t, err, "PutV", tt.name)
		}
	}

	// run Get tests
	for _, tt := range tests["Get"] {
		value := db.Get(tt.buckets[0], tt.keys[0])
		if tt.wantErr {
			assert.Nil(t, value, "Get", tt.name)
		} else {
			assert.Equal(t, tt.value, value, "Get", tt.name)
		}
	}

	// run GetE tests
	for _, tt := range tests["GetE"] {
		value, err := db.GetE(tt.buckets[0], tt.keys[0])
		if tt.wantErr {
			assert.NotNil(t, err, "GetE", tt.name)
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.value, value, "GetE", tt.name)
		}
	}

	// run Delete tests
	for _, tt := range tests["Delete"] {
		err := db.Delete(tt.buckets[0], tt.keys[0])
		if tt.wantErr {
			assert.NotNil(t, err, "Delete", tt.name)
		} else {
			assert.Nil(t, err, "Delete", tt.name)
		}
	}

	// run GetKeys tests
	for _, tt := range tests["GetKeys"] {
		keys := db.GetKeys(tt.buckets[0])
		if tt.wantErr {
			assert.Nil(t, keys, "GetKeys", tt.name)
		} else {
			assert.Equal(t, tt.keys, keys, "GetKeys", tt.name)
		}
	}

	// run GetKeysE tests
	for _, tt := range tests["GetKeysE"] {
		keys, err := db.GetKeysE(tt.buckets[0])
		if tt.wantErr {
			assert.NotNil(t, err, "GetKeysE", tt.name)
		} else {
			assert.Nil(t, err, "GetKeysE", tt.name)
			assert.Equal(t, tt.keys, keys, "GetKeysE", tt.name)
		}
	}

	// run GetBuckets tests
	for _, tt := range tests["GetBuckets"] {
		buckets := db.GetBuckets()
		if tt.wantErr {
			assert.Nil(t, err, "GetBuckets", tt.name)
		} else {
			assert.Equal(t, tt.buckets, buckets, "GetBuckets", tt.name)
		}
	}

	// run DeleteBucket tests
	for _, tt := range tests["DeleteBucket"] {
		err := db.DeleteBucket(tt.buckets[0])
		if tt.wantErr {
			assert.NotNil(t, err, "DeleteBucket", tt.name)
		} else {
			assert.Nil(t, err, "DeleteBucket", tt.name)
		}
	}

}

func TestBDB(t *testing.T) {
	tests := map[string][]struct {
		name    string
		keys    [][]byte
		value   []byte
		wantErr bool
	}{
		"Put": {
			{"simple put key1", [][]byte{[]byte("key1")}, []byte("value1"), false},
			{"simple put key2", [][]byte{[]byte("key2")}, []byte("value2"), false},
			{"auto-increment key", nil, []byte("value2"), false},
		},
		"Get": {
			{"missing key", [][]byte{[]byte("missing")}, nil, true},
			{"valid key", [][]byte{[]byte("key1")}, []byte("value1"), false},
		},
		"GetE": {
			{"missing key", [][]byte{[]byte("missing")}, nil, true},
			{"valid key", [][]byte{[]byte("key1")}, []byte("value1"), false},
		},
		"Delete": {
			{"missing key", [][]byte{[]byte("missing")}, nil, false},
			{"valid key", [][]byte{[]byte("key2")}, nil, false},
		},
		"GetKeys": {
			{"valid bucket", [][]byte{itob(uint64(1)), []byte("key1")}, nil, false},
		},
		"GetKeysE": {
			{"valid bucket", [][]byte{itob(uint64(1)), []byte("key1")}, nil, false},
		},
	}

	defer os.Remove(testdb)

	// start with no database
	_ = os.Remove(testdb)

	// open db
	db, err := OpenB(testdb, testbucket)
	assert.Nil(t, err)
	defer db.Close()

	// run Put tests
	for _, tt := range tests["Put"] {
		var err error
		if tt.keys == nil {
			err = db.Put(nil, tt.value)
		} else {
			err = db.Put(tt.keys[0], tt.value)
		}
		if tt.wantErr {
			assert.NotNil(t, err, "Put", tt.name)
		} else {
			assert.Nil(t, err, "Put", tt.name)
		}
	}

	// run PutV tests
	for _, tt := range tests["PutV"] {
		_, err := db.PutV(tt.value)
		if tt.wantErr {
			assert.NotNil(t, err, "PutV", tt.name)
		} else {
			assert.Nil(t, err, "PutV", tt.name)
		}
	}

	// run Get tests
	for _, tt := range tests["Get"] {
		value := db.Get(tt.keys[0])
		if tt.wantErr {
			assert.Nil(t, value, "Get", tt.name)
		} else {
			assert.Equal(t, tt.value, value, "Get", tt.name)
		}
	}

	// run GetE tests
	for _, tt := range tests["GetE"] {
		value, err := db.GetE(tt.keys[0])
		if tt.wantErr {
			assert.NotNil(t, err, "GetE", tt.name)
		} else {
			assert.Nil(t, err, tt.name)
			assert.Equal(t, tt.value, value, "GetE", tt.name)
		}
	}

	// run Delete tests
	for _, tt := range tests["Delete"] {
		err := db.Delete(tt.keys[0])
		if tt.wantErr {
			assert.NotNil(t, err, "Delete", tt.name)
		} else {
			assert.Nil(t, err, "Delete", tt.name)
		}
	}

	// run GetKeys tests
	for _, tt := range tests["GetKeys"] {
		keys := db.GetKeys()
		if tt.wantErr {
			assert.Nil(t, keys, "GetKeys", tt.name)
		} else {
			assert.Equal(t, tt.keys, keys, "GetKeys", tt.name)
		}
	}

	// run GetKeys tests
	for _, tt := range tests["GetKeysE"] {
		keys, err := db.GetKeysE()
		if tt.wantErr {
			assert.NotNil(t, err, "GetKeysE", tt.name)
		} else {
			assert.Nil(t, err, "GetKeysE", tt.name)
			assert.Equal(t, tt.keys, keys, "GetKeysE", tt.name)
		}
	}
}
