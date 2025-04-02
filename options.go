package ubolt

import (
	"os"
	"time"
)

type Option func(*Database)

func WithOpenFile(fn func(string, int, os.FileMode) (*os.File, error)) Option {
	return func(d *Database) {
		d.openFile = fn
	}
}

func WithTimeout(dur time.Duration) Option {
	return func(d *Database) {
		d.timeout = dur
	}
}
