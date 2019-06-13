package keypool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
)

// default value is same as the default value of related type
type PoolOptions struct {
	RandomRead bool
	ReadOnce   bool

	// -1 means infinite, and 0 means zero
	Size int
}

// this is a random read pool
type Pool struct {
	path string
	ctx  context.Context

	// implement first
	read [][]byte
	// 0 means data not ready, while 1 mean ready
	idx uint32

	write [][]byte

	opt PoolOptions
}

// basic pool based on file
func NewPool(filePath string, opt PoolOptions) (*Pool, error) {
	p := Pool{
		path: filePath,
		opt:  opt,
	}
	if p.opt.Size > 0 {
		p.read = make([][]byte, 0, p.opt.Size)
	}

	if err := p.loadFromFile(); err != nil {
		return nil, err
	}
	if len(p.read) != p.opt.Size && p.opt.Size != -1 {
		return nil, errors.New("put first")
	}
	return &p, nil
}

func (p *Pool) Get() ([]byte, error) {
	idx := atomic.AddUint32(&(p.idx), 1) - 1
	if int(idx) >= p.opt.Size {
		if p.opt.ReadOnce {
			return nil, io.EOF
		} else {
			atomic.StoreUint32(&(p.idx), 1)
			idx = 0
		}
	}
	return p.read[idx], nil
}

func (p *Pool) loadFromFile() error {
	if p.opt.Size == 0 {
		return nil
	}
	file, err := os.OpenFile(p.path, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) && p.opt.Size == -1 {
			return nil
		}
		return err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		p.read = append(p.read, sc.Bytes())
		if len(p.read) >= p.opt.Size && p.opt.Size != -1 {
			break
		}
	}
	return sc.Err()
}

func (p *Pool) flush() (err error) {
	if p.opt.Size > 0 {
		return nil
	}
	existed, err := NewPool(p.path, PoolOptions{Size: -1})
	if err != nil {
		return err
	}
	p.write = append(p.write, existed.read...)
	// merge with exists and shuffle the data
	dest := make([][]byte, len(p.write))
	perm := rand.Perm(len(p.write))
	for i, v := range perm {
		dest[v] = p.write[i]
	}
	p.write = dest

	file, err := os.OpenFile(p.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	for _, bs := range p.write {
		if _, err := file.Write(append(bs, '\n')); err != nil {
			return err
		}
	}
	return file.Sync()
}

// flush to file when finish
// @TODO support new kind of thing
func (p *Pool) PutWorker(ctx context.Context, wg *sync.WaitGroup, ch <-chan []byte) {
	defer wg.Done()
	defer p.flush()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-ch:
			p.write = append(p.write, key)
		}
	}
}
