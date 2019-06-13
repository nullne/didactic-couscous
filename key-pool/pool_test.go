package keypool_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"path"
	"sync"
	"testing"
	"time"

	keypool "github.com/nullne/didactic-couscous/key-pool"
)

func TestPool(t *testing.T) {
	p, err := ioutil.TempDir("/tmp", "keypool")
	if err != nil {
		t.Error(err)
		return
	}
	// defer os.RemoveAll(p)

	ch := make(chan []byte)

	go func() {
		for i := 0; ; i++ {
			time.Sleep(time.Millisecond)
			ch <- []byte(fmt.Sprint(i))
		}
	}()

	pool, err := keypool.NewPool(path.Join(p, "keylist"), keypool.PoolOptions{
		Size: 0,
	})
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wg.Add(1)
	go pool.PutWorker(ctx, &wg, ch)
	wg.Wait()

	size := 1024
	pool, err = keypool.NewPool(path.Join(p, "keylist"), keypool.PoolOptions{
		Size:     size,
		ReadOnce: true,
	})
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wg.Add(1)
	go pool.PutWorker(ctx, &wg, ch)
	for i := 0; i < size; i++ {
		_, err := pool.Get()
		if err != nil {
			t.Error(err)
			return
		}
	}
	wg.Wait()
}
