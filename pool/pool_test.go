package pool_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	// 10*time.Second, 0.01, 50, 20, 20000, 10,
	p := pool.New(ctx, r, pool.Options{
		GetPeriod: 5 * time.Second,
		GetQPS:    50,
		PutQPS:    20,
		MaxPutQPS: 100,
		ReadOnly:  true,
		// ReadOnce:  true,
	})
	go func(ch chan string) {
		for s := range ch {
			log.Println(s)
		}
	}(p.DebugCh())
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, err := p.Get(ctx); err != nil {
					log.Println("get error:", err)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			if err := p.Put(ctx, []byte(fmt.Sprint(i))); err != nil {
				log.Println("put error:", err)
				return
			}
			// fmt.Print(".")
		}
	}()

	wg.Wait()
	fmt.Println(p.Summary())
}
