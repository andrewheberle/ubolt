package ubolt

import (
	"os"
	"time"
)

type Option func(*Database)

// WithOpenFile allows the providing a custom OpenFile function. This is primarily useful for filesystem mocking during tests.
func WithOpenFile(openFile func(string, int, os.FileMode) (*os.File, error)) Option {
	return func(d *Database) {
		d.openFile = openFile
	}
}

// WithTimeout sets the timeout to wait to obtain a lock on a database during Open. A value of 0 will wait forever.
func WithTimeout(timeout time.Duration) Option {
	return func(d *Database) {
		d.timeout = timeout
	}
}

// WithMode sets the file creation mode if a database does not exist.
func WithMode(mode os.FileMode) Option {
	return func(d *Database) {
		d.mode = mode
	}
}
