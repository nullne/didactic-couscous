package main

import (
	"flag"
	"math/rand"
	"os"
)

var (
	fPath = flag.String("p", "/tmp/test-file", "")
	fSize = flag.Int64("s", 1024*1024*100, "")
)

func main() {
	flag.Parse()
	f, err := os.Create(*fPath)
	if err != nil {
		panic(err)
	}
	// if err := os.Truncate(*fPath, *fSize); err != nil {
	// 	panic(err)
	// }
	for {
		bs := make([]byte, 2048)
		_, err := rand.Read(bs)
		if err != nil {
			panic(err)
		}
		_, err = f.Write(bs)
		if err != nil {
			panic(err)
		}
	}

}
