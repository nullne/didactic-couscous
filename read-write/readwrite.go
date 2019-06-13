package main

import (
	"flag"
	"io"
	"math/rand"
	"os"
	"syscall"
	"time"
)

var (
	fPath       = flag.String("p", "/tmp/test-file", "")
	fSize       = flag.Int64("s", 1024*1024*100, "")
	fDirectRead = flag.Bool("d", false, "")
	fwq         = flag.Int("wq", 2000, "")
	fSeek       = flag.Bool("seek", true, "")
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
	if *fDirectRead {
		f, err = os.OpenFile(*fPath, os.O_RDONLY|syscall.O_DIRECT, 0666)
	} else {
		f, err = os.OpenFile(*fPath, os.O_RDONLY|syscall.O_DIRECT, 0666)
	}
	if err != nil {
		panic(err)
	}
	// d int, offset int64, length int64, advice int
	// err = unix.Fadvise(int(f.Fd()), 0, *fSize, unix.FADV_DONTNEED)
	// if err != nil {
	// 	panic(err)
	// }
	if *fwq != 0 {
		limiter := time.NewTicker(time.Duration(float64(time.Second) / float64(*fwq)))
		f, err := os.OpenFile(*fPath, os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		go write(f, limiter.C, *fSeek)
	}

	for {
		length := 1024 * 2
		bs := make([]byte, length)
		offset := rand.Int63n((*fSize - int64(length)) / 512)
		_, err := f.ReadAt(bs, offset*512)
		if err != nil {
			panic(err)
		}
	}
}

func write(f *os.File, ticker <-chan time.Time, seek bool) {
	bs := make([]byte, 1024*64)

	for {
		<-ticker
		rand.Read(bs)
		if seek {
			_, err := f.Seek(0, io.SeekEnd)
			if err != nil {
				panic(err)
			}
		}
		_, err := f.Write(bs)
		if err != nil {
			panic(err)
		}
	}
}
