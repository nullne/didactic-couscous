package pool

type Recorder interface {
	GetN(n int64) ([][]byte, error)
	Rewind() error
	Put([]byte) error
	Close() error
}
