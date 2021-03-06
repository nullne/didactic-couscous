package volume_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/nullne/didactic-couscous/volume"
	"gopkg.in/bufio.v1"
)

func TestVolumeAndFileConcurrently(t *testing.T) {
	volume.MaxFileSize = (1 << 30)
	dir, _ := ioutil.TempDir("/tmp", "nullne_test_volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(dir)
	if err != nil {
		t.Error(err)
	}
	defer v.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sizes := make([]int64, 10)
			for i := 0; i < len(sizes); i++ {
				sizes[i] = mrand.Int63n(1024 * 100)
			}
			for i, size := range sizes {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprintf("%d-%d", i, mrand.Int())
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Errorf("%v: bytes not equal, wanna: %v, got: %v", i, len(data), len(bs))
				}

				// err = v.Delete(file.Info.Fid, "test_file.1")
				// if err != nil {
				// 	t.Error(err)
				// }
				//
				// file3, err := v.Get(file.Info.Fid)
				// if err == nil || file3 != nil {
				// 	t.Error("delete failed?")
				// }
			}
		}()
	}
	wg.Wait()
}

func TestVolumeAndFile(t *testing.T) {
	volume.MaxFileSize = (1 << 10)
	dir, _ := ioutil.TempDir("/tmp", "nullne_test_volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	for idx := 0; idx < 3; idx++ {
		func() {
			v, err := volume.NewVolume(dir)
			if err != nil {
				t.Error(err)
			}
			defer v.Close()

			_, err = volume.NewVolume(dir)
			if err != volume.ErrLockFileExisted {
				t.Error("should not init twice ")
			}

			for i, size := range []int64{1, 100, 1024, 1024 * 1024, 1024 * 1024 * 10, 1024, 100, 1} {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprint(i)
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Error(i, "bytes not equal")
				}

				// err = v.Delete(file.Info.Fid, "test_file.1")
				// if err != nil {
				// 	t.Error(err)
				// }
				//
				// file3, err := v.Get(file.Info.Fid)
				// if err == nil || file3 != nil {
				// 	t.Error("delete failed?")
				// }
			}
		}()
	}
}

func TestVolumeReadFile(t *testing.T) {
	volume.MaxFileSize = (1 << 30)
	dir, _ := ioutil.TempDir("/tmp", "nullne_test_volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(dir)
	if err != nil {
		t.Error(err)
	}
	defer v.Close()

	key := "key"
	data := []byte("0123456789")
	r := bufio.NewBuffer(data)
	if err := v.WriteAll(key, 10, r); err != nil {
		t.Error(err)
		return
	}

	r = bufio.NewBuffer(data)
	if err := v.WriteAll("another KEY", 10, r); err != nil {
		t.Error(err)
		return
	}

	data2, err := v.ReadAll(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, data2) {
		t.Errorf("not equal")
	}

	data3 := make([]byte, 5)
	n, err := v.ReadFile(key, 1, data3)
	if err != nil {
		t.Error(err)
	}
	if n != 5 {
		t.Errorf("not equal")
	}
	if !bytes.Equal(data[1:6], data3) {
		t.Errorf("not equal")
	}

	rr, err := v.ReadFileStream(key, 5, 6)
	if err != nil {
		t.Error(err)
	}
	data4, err := ioutil.ReadAll(rr)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(data[5:], data4) {
		t.Errorf("not equal")
	}

}

func TestVolumeList(t *testing.T) {
	volume.MaxFileSize = (1 << 30)
	dir, _ := ioutil.TempDir("/tmp", "nullne_test_volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(dir)
	if err != nil {
		t.Error(err)
	}
	defer v.Close()

	for i := 30; i > 10; i-- {
		r := bufio.NewBuffer([]byte("nice"))
		key := strings.Join(strings.Split(fmt.Sprint(i), ""), "/") + ".go"

		if err := v.WriteAll(key, 4, r); err != nil {
			t.Error(err)
			return
		}
	}
	entries, err := v.List("", -1)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(entries, ",") != "1/,2/,3/" {
		t.Error("wrong list entries", entries)
	}
}
