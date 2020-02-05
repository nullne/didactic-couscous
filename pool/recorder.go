package pool

import "errors"

type Recorder interface {
	GetN(n int64) ([][]byte, error)
	Rewind() error
	Put([]byte) error
	Close() error
}

var (
	ErrEndOfRecorder = errors.New("end of recorder")
)
