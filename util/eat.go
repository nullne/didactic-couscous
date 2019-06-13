package util

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/awnumar/memguard"
)

func EatPageCache(ctx context.Context, p string, size int64) error {
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	blockSize := 10 * 1024 * 1024
	bs := make([]byte, blockSize)
	for {
		if _, err := f.Read(bs); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	offsetUpper := size - int64(blockSize)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			offset := rand.Int63n(offsetUpper)
			if _, err := f.ReadAt(bs, offset); err != nil {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
	return nil
}

func EatMemory(ctx context.Context, size int) {
	key, err := memguard.NewImmutableRandom(size)
	if err != nil {
		// Oh no, an error. Safely exit.
		fmt.Println(err)
		memguard.SafeExit(1)
	}
	defer key.Destroy()
	<-ctx.Done()
}
