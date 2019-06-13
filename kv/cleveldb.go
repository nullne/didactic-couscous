package main

import (
	"flag"
	"fmt"

	"github.com/jmhodges/levigo"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	fCache       = flag.Int("cache-size", 16, "MB")
	fBloomFilter = flag.Bool("bloom-filter", false, "")
)

func openDB(p string) (*levigo.DB, error) {
	cache := *fCache * opt.MiB

	opt := levigo.NewOptions()
	opt.SetCreateIfMissing(true)

	if *fBloomFilter {
		filter := levigo.NewBloomFilter(10)
		opt.SetFilterPolicy(filter)
	}
	opt.SetWriteBufferSize(cache / 4)
	opt.SetMaxOpenFiles(10000)

	return levigo.Open(p, opt)
}

func closeDB(db *levigo.DB) error {
	db.Close()
	return nil
}

func put(db *levigo.DB, key, value []byte) error {
	opt := levigo.NewWriteOptions()
	return db.Put(opt, key, value)
}

func get(db *levigo.DB, key []byte) ([]byte, error) {
	opt := levigo.NewReadOptions()
	return db.Get(opt, key)
}

func init() {
	fmt.Println("this is C leveldb")
}
