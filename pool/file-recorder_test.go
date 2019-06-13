package pool_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/nullne/didactic-couscous/pool"
)

// may need more nice test case
func TestFileRecorder(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "file-recorder")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fr, err := pool.NewFileRecorder(path.Join(dir, "file-recorder"))
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()

	for i := 0; i < 100; i++ {
		if err := fr.Put([]byte(fmt.Sprintf("nice to meet you %d", i))); err != nil {
			t.Error(err)
			return
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 50000; i++ {
			if err := fr.Put([]byte(fmt.Sprintf("nice to meet you %d", i))); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	for i := 0; i < 100; i++ {
		if bs, err := fr.GetN(20); err != nil {
			t.Error(err)
			return
		} else {
			// for _, b := range bs {
			// 	b
			// 	// fmt.Println(string(b))
			// }
			if len(bs) != 20 {
				if err := fr.Rewind(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	wg.Wait()
}
