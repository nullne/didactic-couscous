package main

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger"
)

// var (
// 	fCache       = flag.Int("cache-size", 16, "MB")
// 	fBloomFilter = flag.Bool("bloom-filter", false, "")
// )

var (
	batch *badger.WriteBatch
)

func init() {
	fmt.Println("this is badger")
}

func openDB(p string) (*badger.DB, error) {
	// cache := *fCache * opt.MiB
	// option := opt.Options{
	// 	// CompactionTableSize: compactionTableSize * opt.MiB,
	// 	BlockCacheCapacity: cache / 2,
	// 	WriteBuffer:        cache / 4,
	// }
	// if *fBloomFilter {
	// 	option.Filter = filter.NewBloomFilter(10)
	// }
	// return leveldb.OpenFile(p, &option)

	opts := badger.DefaultOptions
	opts.Dir = p
	opts.ValueDir = p
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	batch = db.NewWriteBatch()
	return db, nil
}

func closeDB(db *badger.DB) error {
	if err := batch.Flush(); err != nil {
		log.Println(err)
	}
	return db.Close()
}

func put(db *badger.DB, key, value []byte) error {
	return batch.Set(key, value, 0)
}

func get(db *badger.DB, key []byte) ([]byte, error) {
	var ans []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		// Alternatively, you could also use item.ValueCopy().
		ans, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return ans, nil
}
