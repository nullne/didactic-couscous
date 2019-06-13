package main

import (
	"flag"
	"log"
	"math/rand"
	"os"

	"github.com/tecbot/gorocksdb"
)

var (
	fCompressionType = flag.String("compression-type", "snappy", "lz4, snappy")
	fMaxDictBytes    = flag.Int("max-dict-bytes", 0, "")
	fPath            = flag.String("p", "/tmp/test-file", "kv storage path")
	fSize            = flag.Int64("value-size", 200, "")
	fSizeFloor       = flag.Int64("size-floor", 0, "")
	fNumber          = flag.Int("n", 100, "")
	fClear           = flag.Bool("clear", false, "")
)

func clear(paths ...string) {
	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			log.Println(err)
		}
	}
}

func main() {
	flag.Parse()

	if *fClear {
		clear(*fPath)
		return
	}
	opts := gorocksdb.NewDefaultOptions()
	opts.SetMaxOpenFiles(5000)

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(128 << 20))
	opts.SetBlockBasedTableFactory(bbto)

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

	db, err := gorocksdb.OpenDb(opts, *fPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	for i := 0; i < *fNumber; i++ {
		key, _ := getMediaIdentifier()
		size := *fSize
		if *fSizeFloor != 0 {
			size = *fSizeFloor + rand.Int63n(*fSize-*fSizeFloor)
		}
		value := make([]byte, size)
		if _, err := rand.Read(value); err != nil {
			panic(err)
		}
		// fix value
		constant := []byte("application/octet-stream")
		if size >= 100 {
			copy(value, constant)
		}
		if err := db.Put(wo, []byte(key), value); err != nil {
			panic(err)
		}
	}
}

func getMediaIdentifier() (string, error) {
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567" // Based on RFC 4648 Base32 alphabet
	imageName := ""
	bs := make([]byte, 30)
	_, err := rand.Read(bs)
	if err != nil {
		return "", err
	}
	for _, b := range bs {
		imageName += string(alphabet[(int(b) % len(alphabet))])
	}
	return imageName, nil
}
