package ubolt

import "fmt"

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
