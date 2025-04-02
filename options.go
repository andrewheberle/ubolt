package ubolt

import (
	"os"
	"time"
)

type Option func(*Database)

func WithOpenFile(openFile func(string, int, os.FileMode) (*os.File, error)) Option {
	return func(d *Database) {
		d.openFile = openFile
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(d *Database) {
		d.timeout = timeout
	}
}

func WithMode(mode os.FileMode) Option {
	return func(d *Database) {
		d.mode = mode
	}
}
