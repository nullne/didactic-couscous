package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"time"
)

var (
	fCon   = flag.Int("c", 10, "")
	fNum   = flag.Int("n", 8, "")
	fShow  = flag.Int("s", 3000, "")
	fPath  = flag.String("p", "/data4/foo", "")
	fFiles = flag.Int("f", 5, "")
	fSeek  = flag.Bool("seek", true, "")
	fwq    = flag.Int("wq", 2000, "")
	frq    = flag.Int("rq", 2000, "")
)

func main() {
	flag.Parse()
	rand.Seed(int64(time.Now().Nanosecond()))
	files := make([]*os.File, *fFiles)
	for i := 0; i < *fFiles; i++ {
		f, err := os.OpenFile(path.Join(*fPath, fmt.Sprintf("%d.data", i)), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		files[i] = f
	}

	ch := make(chan time.Duration)

	for i := 0; i < *fCon; i++ {
		go func() {
			limiter := time.NewTicker(time.Duration(float64(time.Second) / float64(*frq / *fCon)))
			for {
				<-limiter.C
				ch <- randomRead(files)
			}
		}()
	}

	if *fwq != 0 {
		limiter := time.NewTicker(time.Duration(float64(time.Second) / float64(*fwq)))
		go write(files[0], limiter.C, *fSeek)
	}

	count := 0
	var ta, tb, t time.Duration
	for d := range ch {
		if count%*fNum == 0 {
			tb += t
			t = time.Duration(0)
			if count >= *fShow {
				fmt.Printf("%d avg: %.4f 4-max avg: %.4f \n", *fShow, float64(ta.Seconds())/float64(count), float64(tb.Seconds())/float64(count / *fNum))
				ta = time.Duration(0)
				tb = time.Duration(0)
				count = 0
			}
		}
		ta += d
		count += 1
		if d > t {
			t = d
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

func randomRead(files []*os.File) time.Duration {
	idx := rand.Intn(len(files))
	offset := rand.Int63n(60 * 1024 * 1024 * 1024)
	bs := make([]byte, 16*1024)
	before := time.Now()
	_, err := files[idx].ReadAt(bs, offset)
	if err != nil {
		panic(err)
	}
	return time.Since(before)
}
