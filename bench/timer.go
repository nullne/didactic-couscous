// 传入一个函数，然后可以设定并发执行，然后统计其成功次数，响应时间等等
package bench

import (
	"context"
	"sync"
	"time"
)

func Run(ctx context.Context, fn func() error) {
}

func RunN(ctx context.Context, fn func() error, num int) {
}

func runN(ctx context.Context, fn func() error, num int) {
	w := watcher{
		failed:    make(metric, num),
		succeeded: make(metric, num),
	}
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		go w.run(ctx, &wg, fn, i)
	}

	wg.Add(1)
	go w.show(ctx, &wg)

	wg.Wait()
}

type metric struct {
	count    uint64
	duration time.Duration
}

func (m *metric) observe(duration time.Duration) {
	m.count += 1
	m.duration += duration
}

type watcher struct {
	failed, succeeded []metric
}

func (t *watcher) show(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (t *watcher) run(ctx context.Context, wg *sync.WaitGroup, fn func() error, idx int) time.Duration {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		before := time.Now()
		err := fn()
		duration := time.Since(before)
		if err == nil {
			t.succeeded[idx].observe(duration)
		} else {
			t.failed[idx].observe(duration)
		}
	}
}
