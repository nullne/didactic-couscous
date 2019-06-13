package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	fPath        = flag.String("p", "/tmp/leveldb", "")
	fSize        = flag.Int("s", 200, "")
	fSizeRange   = flag.Int("size-range", 10, "")
	fLength      = flag.Int("l", 1024*1024*1024, "")
	fReadWorker  = flag.Int("r", 100, "")
	fWriteWorker = flag.Int("w", 100, "")
	fTicker      = flag.Duration("t", 10*time.Second, "")
)

func main() {
	flag.Parse()
	db, err := leveldb.OpenFile(*fPath, &opt.Options{})
	if err != nil {
		panic(err)
	}

	size := func() int {
		if *fSizeRange == 0 {
			return *fSize
		}
		return rand.Intn(*fSizeRange) + *fSize
	}

	wch, rch := pool(*fLength, *fWriteWorker == 0)
	rlatency := latency("read", *fTicker)
	wlatency := latency("write", *fTicker)
	for i := 0; i < *fWriteWorker; i++ {
		go write(db, wch, wlatency, size)
	}

	for i := 0; i < *fReadWorker; i++ {
		go read(db, rch, rlatency)
	}

	ch := make(chan struct{})
	<-ch
}

func latency(name string, d time.Duration) chan time.Duration {
	ch := make(chan time.Duration)
	go func() {
		ticker := time.NewTicker(d)
		var total, tt time.Duration
		var count, cc int64
		for lat := range ch {
			count++
			cc++
			total += lat
			tt += lat
			select {
			case <-ticker.C:
				fmt.Printf("total %s %15d, last %10d in %30s, qps %.2f, avg %30s \n", name, count, cc, tt.String(), float64(cc)/d.Seconds(), time.Duration(tt.Nanoseconds()/cc).String())
				tt = time.Duration(0)
				cc = 0
			default:
			}
		}
	}()
	return ch
}

func pool(length int, readOnly bool) (chan []byte, chan []byte) {
	ch := make(chan []byte)
	ch2 := make(chan []byte)
	pool := make([][]byte, 0, length)

	go func() {
		flag := false
		for key := range ch {
			if len(pool) < cap(pool) {
				pool = append(pool, key)
				continue
			}
			if !flag {
				fmt.Println("yes!")
				flag = true
			}

			idx := rand.Intn(len(pool))
			pool[idx] = key
		}
	}()

	go func() {
		for {
			if len(pool) == 0 {
				time.Sleep(time.Second)
				continue
			}
			idx := rand.Intn(len(pool))
			ch2 <- pool[idx]
		}
	}()
	return ch, ch2
}

func read(db *leveldb.DB, ch chan []byte, latency chan time.Duration) {
	for key := range ch {
		before := time.Now()
		_, err := db.Get(key, nil)
		if err != nil {
			panic(err)
		}
		latency <- time.Since(before)
	}
}

func write(db *leveldb.DB, ch chan []byte, latency chan time.Duration, size func() int) {
	for {
		id, _ := getMediaIdentifier()
		key := []byte(id)
		value := make([]byte, size())
		if _, err := rand.Read(value); err != nil {
			panic(err)
		}
		before := time.Now()
		if err := db.Put(key, value, nil); err != nil {
			panic(err)
		}
		latency <- time.Since(before)
		ch <- key
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
