// 1. 以 maxPutQPS 的速度写入
// 2. 写入一定数量之后，开始以 getQPS, putQPS 分别进行读写
// 3. 读取的内容为之前写入的数据，按照 sampleRatio （抽样率）进行抽样后随机处理，然后再进行去取。如果读取的速率大于写入的速率，则倒带至最开始
// 4. 顺便告诉需要等多久

package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

var (
	ErrReadOnce = errors.New("the pool should only be read once")
	ErrDone     = errors.New("the pool is closed/finished")
)

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

	NotShuffle bool
}

func (opt Options) prefillSize() float64 {
	if opt.ReadOnly {
		return opt.poolSize() / opt.SampleRatio
	}
	if opt.PutQPS >= opt.GetQPS {
		return opt.poolSize() / opt.SampleRatio
	} else {
		return opt.poolSize() / opt.SampleRatio * (opt.GetQPS / opt.PutQPS)
	}
}

func (opt Options) poolSize() float64 {
	return opt.GetQPS * opt.GetPeriod.Seconds()
}

func (opt Options) getSize() int64 {
	return int64(1 / opt.SampleRatio)
}

func (opt *Options) setDefault() {
	if opt.GetPeriod == 0 {
		opt.GetPeriod = time.Minute
	}
	if opt.SampleRatio <= 0 {
		opt.SampleRatio = 1
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

type Pool struct {
	pools      [2]pool
	recorder   Recorder
	remainings [][]byte

	get chan []byte
	put chan []byte

	counter struct {
		get     uint64
		put     uint64
		round   uint64
		prefill uint64
	}

	logCh chan string

	closePut chan struct{}

	poolSize float64
	getSize  int64
	options  Options
}

func New(ctx context.Context, recorder Recorder, opt Options) *Pool {
	opt.setDefault()
	if opt.ReadOnce && !opt.ReadOnly {
		panic("ReadOnly must be set to true when ReadOnce is true")
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	dataCh := make(chan []byte)
	p := Pool{
		poolSize: opt.poolSize(),
		get:      make(chan []byte),
		put:      dataCh,
		getSize:  opt.getSize(),
		recorder: recorder,
		options:  opt,
		logCh:    make(chan string, 1000),
		closePut: make(chan struct{}),
	}

	p.pools[0] = newPool(int(p.poolSize))
	p.pools[1] = newPool(int(p.poolSize))

	prefillSize := opt.prefillSize()
	p.debug("prefill size: %v, pool size: %v\n", prefillSize, p.poolSize)

	go func() {
		if err := p.prefill(ctx, prefillSize); err != nil {
			p.debug("failed to prefill: %v", err)
			close(p.get)
			close(p.closePut)
			return
		}
		go p.fillPools(ctx, opt.MaxGetQPS, opt.Burst)
		go p.getWorker(ctx, opt.GetQPS, opt.Burst)
		if opt.ReadOnly {
			close(p.closePut)
		} else {
			go p.putWorker(ctx, opt.PutQPS, opt.Burst)
		}
	}()
	return &p
}

func (p *Pool) Put(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.put <- data:
		return nil
	case <-p.closePut:
		return ErrDone
	}
}

func (p *Pool) Get(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data, ok := <-p.get:
		if !ok {
			return nil, ErrDone
		}
		return data, nil
	}
}

func (p *Pool) debug(format string, a ...interface{}) {
	select {
	case p.logCh <- fmt.Sprintf(format, a...):
	default:
		// discard message when full
	}
}

// return debug message channel from which debug message can be read
func (p *Pool) DebugCh() chan string {
	return p.logCh
}

func (p *Pool) Summary() string {
	return fmt.Sprintf("get %d, put %d, prefill %d",
		atomic.LoadUint64(&(p.counter.get)),
		atomic.LoadUint64(&(p.counter.put)),
		atomic.LoadUint64(&(p.counter.prefill)),
	)
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

func (p *Pool) prefill(ctx context.Context, prefillSize float64) error {
	p.debug("start prefill %v", prefillSize)
	defer p.debug("finish prefill")

	// step 1: read from existed
	need, rcounter, err := p.needPrefill(ctx, prefillSize)
	if err != nil {
		return err
	}
	if !need {
		return nil
	}

	// step 2: put in MaxPutQPS
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.putWorker(ctx2, p.options.MaxPutQPS, p.options.Burst)

	remains := uint64(prefillSize) - rcounter
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if atomic.LoadUint64(&(p.counter.put)) >= remains {
				return nil
			}
		}
	}
	return nil
}

func (p *Pool) needPrefill(ctx context.Context, prefillSize float64) (bool, uint64, error) {
	defer p.recorder.Rewind()
	batchSize := float64(1)
	if batchSize > prefillSize {
		batchSize = prefillSize
	}

	var rcounter uint64
	rlimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(p.options.MaxGetQPS/batchSize)), int(p.options.Burst))
	for {
		if err := rlimiter.Wait(ctx); err != nil {
			return false, 0, err
		}
		select {
		case <-ctx.Done():
			return false, 0, ctx.Err()
		default:
		}
		bs, err := p.recorder.GetN(int64(batchSize))
		if err != nil {
			return false, 0, err
		}
		rcounter += uint64(len(bs))
		if float64(rcounter+atomic.LoadUint64(&(p.counter.put))) >= prefillSize {
			p.debug("the existed is enough to fill the pool")
			return false, 0, nil
		}
		// read to EOF
		if len(bs) < int(batchSize) {
			p.debug("read %v from the existed", rcounter)
			break
		}
	}
	return true, rcounter, nil
}

func (p *Pool) putWorker(ctx context.Context, putQPS, burst float64) {
	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(putQPS)), int(burst))
	for {
		limiter.Wait(ctx)
		select {
		case <-ctx.Done():
			return
		case data := <-p.put:
			if err := p.recorder.Put(data); err != nil {
				p.debug("failed to out into recorder: %v", err)
				time.Sleep(time.Second)
				continue
			}
			atomic.AddUint64(&(p.counter.put), 1)
		}
	}
}

func (p *Pool) getWorker(ctx context.Context, getQPS, burst float64) {
	defer close(p.get)
	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(getQPS)), int(burst))
	idx := 0
Outer:
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-p.pools[idx].readable:
			if !ok {
				break Outer
			}
		}
		p.debug("start to read %v", idx)
		before := time.Now()
		for _, key := range p.pools[idx].data {
			if err := limiter.Wait(ctx); err != nil {
				p.debug("limiter error: %v", err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case p.get <- key:
				atomic.AddUint64(&(p.counter.get), 1)
			}
		}
		p.debug("last %v reading %v", time.Since(before), idx)
		p.pools[idx].writable <- struct{}{}
		idx = (idx + 1) % 2
	}

	// remaines
	if len(p.remainings) == 0 {
		return
	}
	p.debug("read remainings")
	for _, key := range p.remainings {
		if err := limiter.Wait(ctx); err != nil {
			p.debug("limiter 2 error: %v", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case p.get <- key:
			atomic.AddUint64(&(p.counter.get), 1)
		}
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
		p.debug("fill the pool %v", idx)
		//fill the pool
		if err := p.fillPool(ctx, p.pools[idx].data, qps, burst); err != nil {
			if err == ErrReadOnce {
				p.debug("stop filling the pool for ReadOnce")
				close(p.pools[idx].readable)
				return
			}
			p.debug("failed to fill the pool %v: %v, will try later", idx, err)
			time.Sleep(time.Second)
			p.pools[idx].writable <- struct{}{}
			continue
		}
		p.pools[idx].readable <- struct{}{}
		idx = (idx + 1) % 2
	}
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
			if p.options.ReadOnce {
				p.remainings = data[:i]
				if err := shuffle(p.remainings); err != nil {
					p.debug("failed to shuffle: %v", err)
				}
				return ErrReadOnce
			}
			if err := p.recorder.Rewind(); err != nil {
				return err
			}
			p.debug("rewind to the beginning")
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
	if p.options.NotShuffle {
		return nil
	}
	return shuffle(data)
}

func shuffle(data [][]byte) error {
	dest := make([][]byte, len(data))
	perm := rand.Perm(len(data))
	for i, v := range perm {
		dest[v] = data[i]
	}
	if n := copy(data, dest); n != len(data) {
		return fmt.Errorf("failed to copy, got %v, want %v", n, len(data))
	}
	return nil
}
