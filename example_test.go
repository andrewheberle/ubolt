package ubolt_test

import (
	"fmt"

	"github.com/andrewheberle/ubolt"
)

func ExampleBucket_Get() {
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
