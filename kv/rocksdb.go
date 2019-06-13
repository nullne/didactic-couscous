package main

import (
	"flag"
	"fmt"

	"github.com/tecbot/gorocksdb"
)

var (
	fBloomFilter     = flag.Bool("bloom-filter", false, "")
	fTableFactory    = flag.String("table-factory", "blocked", "blocked or plain")
	fDirectRead      = flag.Bool("direct-read", false, "")
	fBlockCache      = flag.Int("block-cache", 3, "M")
	fMaxDictBytes    = flag.Int("max-dict-bytes", 0, "")
	fCompressionType = flag.String("compression-type", "snappy", "lz4, snappy")
)

func openDB(p string) (*gorocksdb.DB, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetMaxOpenFiles(5000)

	if *fDirectRead {
		opts.SetUseDirectReads(true)
	}

	switch *fTableFactory {
	case "blocked":
		bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
		if *fBlockCache == 0 {
			bbto.SetNoBlockCache(true)
		} else {
			bbto.SetBlockCache(gorocksdb.NewLRUCache(*fBlockCache << 20))
		}
		if *fBloomFilter {
			fp := gorocksdb.NewBloomFilter(10)
			bbto.SetFilterPolicy(fp)
		}
		opts.SetBlockBasedTableFactory(bbto)
	case "plain":
		opts.SetPlainTableFactory(0, 10, 0.75, 16)
		pt := gorocksdb.NewFixedPrefixTransform(6)
		opts.SetPrefixExtractor(pt)
	}

	switch *fCompressionType {
	case "snappy":
		opts.SetCompression(gorocksdb.SnappyCompression)
	case "lz4":
		opts.SetCompression(gorocksdb.LZ4Compression)
	default:
		opts.SetCompression(gorocksdb.SnappyCompression)
	}

	copt := gorocksdb.NewDefaultCompressionOptions()
	copt.MaxDictBytes = *fMaxDictBytes
	opts.SetCompressionOptions(copt)

	opts.SetCreateIfMissing(true)
	return gorocksdb.OpenDb(opts, p)
}

var (
	wo = gorocksdb.NewDefaultWriteOptions()
	ro = gorocksdb.NewDefaultReadOptions()
)

func closeDB(db *gorocksdb.DB) error {
	db.Close()
	wo.Destroy()
	ro.Destroy()
	return nil
}

func put(db *gorocksdb.DB, key, value []byte) error {
	return db.Put(wo, key, value)
}

func get(db *gorocksdb.DB, key []byte) ([]byte, error) {
	value, err := db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	defer value.Free()
	bs := make([]byte, value.Size())
	copy(bs, value.Data())
	return bs, nil
}

func init() {
	fmt.Println("this is rocksdb")
}
