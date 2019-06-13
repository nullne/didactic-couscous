package main

import (
	"flag"
	"math/rand"
	"os"
	"syscall"
)

var (
	fPath = flag.String("p", "/tmp/test-file", "")
	fSize = flag.Int64("s", 1024*1024*100, "")
)

func main() {
	flag.Parse()
	f, err := os.Create(*fPath)
	if err != nil {
		panic(err)
	}
	if err := os.Truncate(*fPath, *fSize); err != nil {
		panic(err)
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
	f, err = os.OpenFile(*fPath, os.O_RDONLY|syscall.O_DIRECT, 0666)
	if err != nil {
		panic(err)
	}
	// d int, offset int64, length int64, advice int
	// err = unix.Fadvise(int(f.Fd()), 0, *fSize, unix.FADV_DONTNEED)
	// if err != nil {
	// 	panic(err)
	// }

	for {
		length := 1023 * 2
		bs := make([]byte, length)
		offset := rand.Int63n((*fSize - int64(length)) / 512)
		_, err := f.ReadAt(bs, offset)
		if err != nil {
			panic(err)
		}
	}

}
