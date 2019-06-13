package pool

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
)

type FileRecorder struct {
	write   *os.File
	read    *os.File
	scanner *bufio.Scanner
}

func NewFileRecorder(p string) (*FileRecorder, error) {
	f1, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	f2, err := os.OpenFile(p, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileRecorder{
		read:    f2,
		write:   f1,
		scanner: bufio.NewScanner(f2),
	}, nil
}

func (r *FileRecorder) GetN(n int64) ([][]byte, error) {
	data := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		if !r.scanner.Scan() {
			return data, r.scanner.Err()
		}
		bs := make([]byte, len(r.scanner.Bytes()))
		copy(bs, r.scanner.Bytes())
		data = append(data, bs)
	}
	return data, r.scanner.Err()
}

func (r *FileRecorder) Rewind() error {
	_, err := r.read.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	r.scanner = bufio.NewScanner(r.read)
	return nil
}

func (r *FileRecorder) Put(data []byte) error {
	if bytes.Contains(data, []byte{'\n'}) {
		return errors.New("new line should not be contained in data")
	}
	if _, err := r.write.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	_, err := r.write.Write(append(data, '\n'))
	return err
}

func (r *FileRecorder) Close() error {
	r.read.Close()
	return r.write.Close()
}
