package main

import (
	"flag"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	fCache       = flag.Int("cache-size", 16, "MB")
	fBloomFilter = flag.Bool("bloom-filter", false, "")
)

func openDB(p string) (*leveldb.DB, error) {
	cache := *fCache * opt.MiB
	option := opt.Options{
		// CompactionTableSize: compactionTableSize * opt.MiB,
		BlockCacheCapacity: cache / 2,
		WriteBuffer:        cache / 4,
	}
	if *fBloomFilter {
		option.Filter = filter.NewBloomFilter(10)
	}
	return leveldb.OpenFile(p, &option)
}

func closeDB(db *leveldb.DB) error {
	return db.Close()
}

func put(db *leveldb.DB, key, value []byte) error {
	return db.Put(key, value, nil)
}

func get(db *leveldb.DB, key []byte) ([]byte, error) {
	return db.Get(key, nil)
}

func init() {
	fmt.Println("this is go leveldb")
}
