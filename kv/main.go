// 测试不同kv 的读写性能，写入字符串，随机只读一次,  还需要测试重启
// 1. 准备阶段， 灌入数据
// 2. 在指定的 QPS 下测试响应时间
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/nullne/didactic-couscous/pool"
	"github.com/nullne/didactic-couscous/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// implement follow functions and place the related file behind the main.go when building
// db, err := openDB() (*DB, error)
// err := put(db, key, value)
// value, err := get(db, key)
// close(db)

// sync please

var (
	fPath      = flag.String("p", "/tmp/test-file", "kv storage path")
	fListPath  = flag.String("key-list-path", "/tmp/key-list", "")
	fSize      = flag.Int64("value-size", 150, "")
	fSizeFloor = flag.Int64("size-floor", 50, "")
	// fReadPoolSize = flag.Int("read-pool-size", 1000000, "test performance as the storage level arise")

	fDuration = flag.Duration("duration", 7*24*time.Hour, "")
	// fPutNumber = flag.Int("put-number", 1000000, "")

	// qps & concurrent
	fwq          = flag.Float64("wq", 500, "write qps")
	fwqMax       = flag.Float64("wq-max", -1, "max write qps, -1 means no limit")
	fwc          = flag.Int("wc", 200, "write concurrent")
	frq          = flag.Float64("rq", 2500, "read qps")
	frc          = flag.Int("rc", 200, "read concurrent")
	fBurst       = flag.Float64("burst", 100, "")
	fReadPeriod  = flag.Duration("read-period", time.Minute, "")
	fSampleRatio = flag.Float64("sample-ratio", 0.01, "")

	fJobNumbs = flag.Int("job-num", 1, "")

	fPreallocateMemory = flag.Int("mem", 0, "pre allocate memory(GB)")
	fEatPageSize       = flag.Int64("page-size", 0, "")
	fPageSizeFile      = flag.String("page-size-file", "/tmp/big-file", "")

	fClear = flag.Bool("clear", false, "")
)

func clear(paths ...string) {
	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			log.Println(err)
		}
	}
}

func mkdir(p string) error {
	_, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(p, 0777)
		}
		return err
	}
	return nil
}

func main() {
	flag.Parse()
	// if *frq == 0 {
	// 	*fReadPoolSize = 0
	// }

	if *fClear {
		clear(*fPath, *fListPath)
		return
	}

	initPrometheus()
	ctx, cancel := context.WithTimeout(context.Background(), *fDuration)
	defer cancel()

	if *fPreallocateMemory != 0 {
		go util.EatMemory(ctx, *fPreallocateMemory*(1<<30))
	}

	if *fEatPageSize != 0 {
		if err := util.EatPageCache(ctx, *fPageSizeFile, *fEatPageSize*(1<<30)); err != nil {
			panic(err)
		}
		fmt.Println("eat all page cache and keep eating")
	}

	var wg sync.WaitGroup
	// writeCh := make(chan []byte)
	r, err := pool.NewFileRecorder(*fListPath)
	if err != nil {
		panic(err)
	}
	p := pool.New(ctx, r, pool.Options{
		GetPeriod:   *fReadPeriod,
		SampleRatio: *fSampleRatio,
		GetQPS:      *frq,
		PutQPS:      *fwq,
		MaxPutQPS:   *fwqMax,
		Burst:       *fBurst,
	})
	// defer p.Close()
	// pool, err := keypool.NewPool(*fListPath, keypool.PoolOptions{
	// 	ReadOnce: true,
	// 	Size:     *fReadPoolSize,
	// })

	// if err != nil {
	// 	panic(err)
	// }

	// if *fwc != 0 {
	// 	wg.Add(1)
	// 	go pool.PutWorker(ctx, &wg, writeCh)
	// }

	// var counter int32
	// go func(ctx context.Context) {
	// 	for _ = range time.Tick(time.Second) {
	// 		c := atomic.LoadInt32(&counter)
	// 		if int(c) >= *fPutNumber {
	// 			cancel()
	// 		}
	// 	}
	// }(ctx)

	var puts []func(key, value []byte) error
	var gets []func(key []byte) ([]byte, error)

	for idx := 0; idx < *fJobNumbs; idx++ {
		dbPath := path.Join(*fPath, fmt.Sprint(idx))
		if err := mkdir(dbPath); err != nil {
			panic(err)
		}
		db, err := openDB(dbPath)
		if err != nil {
			panic(err)
		}
		defer closeDB(db)
		puts = append(puts, func(key, value []byte) error {
			return put(db, key, value)
		})

		gets = append(gets, func(key []byte) ([]byte, error) {
			return get(db, key)
		})
	}

	putFn := func() error {
		key, _ := getMediaIdentifier()
		size := *fSize
		if *fSizeFloor != 0 {
			size = *fSizeFloor + rand.Int63n(*fSize-*fSizeFloor)
		}
		value := make([]byte, size)
		if _, err := rand.Read(value); err != nil {
			return err
		}
		// fix value
		constant := []byte("application/octet-stream")
		if size >= 100 {
			copy(value, constant)
		}
		for _, put := range puts {
			before := time.Now()
			err := put([]byte(key), value)
			requestsDuration.With(prometheus.Labels{"operation_type": "put"}).Observe(time.Since(before).Seconds())
			if err != nil {
				return err
			}
		}
		if err := p.Put(ctx, []byte(key)); err != nil {
			return err
		}
		return nil
	}

	getFn := func() error {
		key, err := p.Get(ctx)
		if err != nil {
			return err
		}
		for _, get := range gets {
			before := time.Now()
			_, err = get(key)
			requestsDuration.With(prometheus.Labels{"operation_type": "get"}).Observe(time.Since(before).Seconds())
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < *fwc && *fwq != 0; i++ {
		wg.Add(1)
		go worker(ctx, &wg, putFn)
	}

	for i := 0; i < *frc && *frq != 0; i++ {
		wg.Add(1)
		go worker(ctx, &wg, getFn)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	{
		for {
			select {
			case <-signalCh:
				cancel()
				log.Println("will exist by signal, wait all routines finished")
				wg.Wait()
				return
			case <-ctx.Done():
				log.Println("mission complete")
				wg.Wait()
				return
			}
		}
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, fn func() error) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := fn(); err != nil {
			log.Println("worker exist: ", err)
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

var (
	requestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_operation",
			Help:    "Time taken by requests served by current Minio server instance",
			Buckets: []float64{.001, .003, .005, .01, .02, .03, .05, .1},
		},
		[]string{"operation_type"},
	)
)

func initPrometheus() {
	prometheus.MustRegister(requestsDuration)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":2112", nil)
}
