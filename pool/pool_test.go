package pool_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/nullne/didactic-couscous/pool"
)

func TestPool(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "file-recorder")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	r, err := pool.NewFileRecorder(path.Join(dir, "file-recorder"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	// 10*time.Second, 0.01, 50, 20, 20000, 10,
	p := pool.New(ctx, r, pool.Options{
		GetPeriod: 10 * time.Second,
		GetQPS:    50,
		PutQPS:    20,
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		go func() {
			for {
				p.Get(ctx)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			p.Put(ctx, []byte(fmt.Sprint(i)))
		}
	}()

	wg.Wait()
}
