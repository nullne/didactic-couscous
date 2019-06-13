// 1. 以 maxPutQPS 的速度写入
// 2. 写入一定数量之后，开始以 getQPS, putQPS 分别进行读写
// 3. 读取的内容为之前写入的数据，按照 sampleRatio （抽样率）进行抽样后随机处理，然后再进行去取。如果读取的速率大于写入的速率，则倒带至最开始
// 4. 顺便告诉需要等多久

package pool

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type Pool struct {
	prefillSize float64
	poolSize    float64
	getSize     int64

	pools    [2]pool
	recorder Recorder

	get chan []byte
	put chan []byte
}

func New(ctx context.Context, recorder Recorder, opt Options) *Pool {
	opt.setDefault()
	rand.Seed(int64(time.Now().Nanosecond()))
	dataCh := make(chan []byte)
	p := Pool{
		poolSize: opt.GetQPS * opt.GetPeriod.Seconds(),
		get:      make(chan []byte),
		put:      dataCh,
		getSize:  int64(1 / opt.SampleRatio),
		recorder: recorder,
	}

	if opt.PutQPS > opt.GetQPS {
		p.prefillSize = p.poolSize / opt.SampleRatio
	} else {
		p.prefillSize = p.poolSize / opt.SampleRatio * (opt.GetQPS / opt.PutQPS)
	}

	p.pools[0] = newPool(int(p.poolSize))
	p.pools[1] = newPool(int(p.poolSize))
	log.Printf("prefill or read size: %v, pool size: %v\n", p.prefillSize, p.poolSize)

	go func() {
		log.Println("start prefill", p.prefillSize)
		if err := p.prefill(ctx, dataCh, opt.MaxPutQPS, opt.MaxGetQPS, opt.Burst); err != nil {
			log.Println("failed to prefill: ", err)
			return
		}
		log.Println("finish prefill")
		go p.fillPools(ctx, opt.MaxGetQPS, opt.Burst)
		go p.putWorker(ctx, opt.PutQPS, opt.Burst, dataCh)
		go p.getWorker(ctx, opt.GetQPS, opt.Burst)
	}()
	return &p
}

func (p *Pool) Put(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.put <- data:
		return nil
	}
}

func (p *Pool) Get(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-p.get:
		return data, nil
	}
}

type Options struct {
	GetPeriod   time.Duration
	SampleRatio float64
	GetQPS      float64
	PutQPS      float64
	// rate to fill the pool
	MaxGetQPS float64
	// rate to prefill the pool
	MaxPutQPS float64
	Burst     float64

	ReadOnly bool
	ReadOnce bool
}

func (opt *Options) setDefault() {
	if opt.GetPeriod == 0 {
		opt.GetPeriod = time.Minute
	}
	if opt.SampleRatio <= 0 {
		opt.SampleRatio = 0.01
	}
	if opt.GetQPS == 0 {
		opt.GetQPS = 10
	}
	if opt.PutQPS == 0 {
		opt.PutQPS = 10
	}
	if opt.MaxPutQPS == 0 {
		opt.MaxPutQPS = -1
	}
	if opt.MaxGetQPS == 0 {
		opt.MaxGetQPS = -1
	}
	if opt.Burst <= 0 {
		opt.Burst = 1
	}
}

type pool struct {
	data               [][]byte
	readable, writable chan struct{}
}

func newPool(size int) pool {
	p := pool{
		data:     make([][]byte, size),
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
	}
	p.writable <- struct{}{}
	return p
}

func (p *Pool) prefill(ctx context.Context, ch <-chan []byte, wqps, rqps, burst float64) error {
	var counter uint64
	finish := make(chan error, 1)
	go func() (err error) {
		defer log.Println("finish real prefill")
		defer func() {
			finish <- err
		}()
		wlimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(wqps)), int(burst))
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case key, ok := <-ch:
				if !ok {
					return errors.New("channel was closed")
				}
				if err := p.recorder.Put(key); err != nil {
					return err
				}
				if c := atomic.AddUint64(&counter, 1); float64(c) >= p.prefillSize {
					return nil
				}
			}
			if err := wlimiter.Wait(ctx); err != nil {
				return err
			}
		}
	}()

	defer p.recorder.Rewind()

	var rcounter uint64
	rlimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(rqps)), int(burst))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-finish:
			return err
		default:
		}
		bs, err := p.recorder.GetN(100)
		if err != nil {
			return err
		}
		rcounter += uint64(len(bs))
		if float64(rcounter) >= p.prefillSize {
			atomic.StoreUint64(&counter, rcounter)
			log.Println("read all from previous")
			return nil
		}
		// read to EOF
		if len(bs) < 100 {
			atomic.StoreUint64(&counter, rcounter)
			log.Printf("read %v from previous\n", rcounter)
			break
		}
		if err := rlimiter.Wait(ctx); err != nil {
			return err
		}
	}
	return <-finish
}

func (p *Pool) putWorker(ctx context.Context, putQPS, burst float64, ch <-chan []byte) {
	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(putQPS)), int(burst))
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				log.Println("finish put worker")
				return
			}
			if err := p.recorder.Put(data); err != nil {
				log.Println(err)
				time.Sleep(time.Second)
			}
		}
		limiter.Wait(ctx)
	}
}

func (p *Pool) getWorker(ctx context.Context, getQPS, burst float64) {
	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(getQPS)), int(burst))
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.pools[idx].readable:
		}
		log.Println("start to read", idx)
		before := time.Now()
		for _, key := range p.pools[idx].data {
			if err := limiter.Wait(ctx); err != nil {
				log.Println(err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case p.get <- key:
			}
		}
		log.Println("end read", idx, time.Since(before))
		p.pools[idx].writable <- struct{}{}
		idx = (idx + 1) % 2
	}
}

func (p *Pool) fillPools(ctx context.Context, qps, burst float64) {
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.pools[idx].writable:
		}
		log.Println("fill the pool", idx)
		//fill the pool
		if err := p.fillPool(ctx, p.pools[idx].data, qps, burst); err != nil {
			log.Printf("failed to fill the pool %v: %v, will try later\n", idx, err)
			time.Sleep(time.Second)
			p.pools[idx].writable <- struct{}{}
			continue
		}
		// if err := p.dump(idx); err != nil {
		// 	time.Sleep(time.Second)
		// 	p.pools[idx].writable <- struct{}{}
		// 	continue
		// }
		p.pools[idx].readable <- struct{}{}
		idx = (idx + 1) % 2
	}
}

func (p *Pool) dump(idx int) error {
	dir, err := ioutil.TempDir("/tmp", "oss-key")
	if err != nil {
		return err
	}
	fp := path.Join(dir, "key-file")
	f, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer f.Close()
	log.Println("dump key file to ", fp, idx)
	for _, key := range p.pools[idx].data {
		if _, err := f.Write(append(key, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pool) fillPool(ctx context.Context, data [][]byte, qps, burst float64) error {
	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(qps)), int(burst))
	for i := 0; i < len(data); i++ {
		bs, err := p.recorder.GetN(p.getSize)
		if err != nil {
			return err
		}
		if int64(len(bs)) != p.getSize {
			// need rewind and get more
			if err := p.recorder.Rewind(); err != nil {
				return err
			}
			log.Println("rewind to the beginning")
			bs2, err := p.recorder.GetN(p.getSize - int64(len(bs)))
			if err != nil {
				return err
			}
			bs = append(bs, bs2...)
		}
		data[i] = bs[rand.Int63n(p.getSize)]

		if err := limiter.Wait(ctx); err != nil {
			return err
		}
	}

	//shuffle the data
	dest := make([][]byte, len(data))
	perm := rand.Perm(len(data))
	for i, v := range perm {
		dest[v] = data[i]
	}
	if n := copy(data, dest); float64(n) != p.poolSize {
		return fmt.Errorf("failed to copy, got %v, want %v", n, p.poolSize)
	}
	return nil
}
