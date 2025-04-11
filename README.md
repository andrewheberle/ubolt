# ubolt

[![Go Report Card](https://goreportcard.com/badge/github.com/andrewheberle/ubolt?style=flat)](https://goreportcard.com/report/github.com/andrewheberle/ubolt)
[![Godoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat)](https://godoc.org/github.com/andrewheberle/ubolt)
[![tag](https://img.shields.io/github/v/tag/andrewheberle/ubolt)](https://github.com/andrewheberle/ubolt/-/tags)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue)](https://github.com/andrewheberle/ubolt/-/blob/main/LICENSE)
[![codecov](https://codecov.io/gh/andrewheberle/ubolt/graph/badge.svg?token=62CL49FG06)](https://codecov.io/gh/andrewheberle/ubolt)

ubolt is a convenience wrapper around various [github.com/etcd-io/bbolt](https://github.com/etcd-io/bbolt) calls.

## Differences

All calls such as `Get` or `Put` are automatically run within a transaction, to ensure consistent reads/writes for that call.

In addition the `OpenBucket` function allows the opening of a database and creation of a bucket in one call, with subsequent calls to `Put`, `Get` etc being made against the named bucket automatically.

If lower level access is required the underlying `bbolt.DB` is available as `BoltDB()`, however in this case it is up to the user to ensure transactions are used and reads/writes are against the expected bucket.

Additional calls such as `PutV`, which inserts a value using a generated key, along with `GetE` and `PutE` variants of `Get` and `Put` which return an error in all cases, such as `ErrKeyNotFound` rather than a `nil` value. 

## Usage

The following example shows opening a bucket and reading and writing a single key.

```go
package main

import "github.com/andrewheberle/ubolt"

func main() {
	bucket := []byte("mybucket")
	key := []byte("mykey")
	value := []byte("myvalue")

	db, err := ubolt.OpenBucket("database.db", bucket)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Put(key, value); err != nil {
		panic(err)
	}

	v, err := db.GetE(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", v)
	// Output: myvalue
}
```
