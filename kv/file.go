package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"

	"github.com/nullne/didactic-couscous/volume"
)

type file struct {
	file *os.File
	lock sync.Mutex
	size int64
}

func openDB(p string) (*file, error) {
	f, err := os.OpenFile(path.Join(p, "data"), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	if err := volume.Fallocate(int(f.Fd()), 0, 64*1024*1024*1024); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &file{
		file: f,
		size: info.Size(),
	}, nil
}

func closeDB(db *file) error {
	return db.file.Close()
}

func put(db *file, key, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	_, err := db.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	_, err = db.file.Write(value)
	if err != nil {
		return err
	}
	return nil
}

func get(db *file, key []byte) ([]byte, error) {
	if db.size == 0 {
		return nil, errors.New("fuck zero")
	}
	bs := make([]byte, *fSize)
	offset := rand.Int63n(db.size / *fSize - 1) * *fSize
	_, err := db.file.ReadAt(bs, offset)
	return nil, err
}

func init() {
	fmt.Println("this is file volume")
}
