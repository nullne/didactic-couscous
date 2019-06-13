package main

import (
	"fmt"
	"path"

	"github.com/boltdb/bolt"
)

func openDB(p string) (*bolt.DB, error) {
	return bolt.Open(path.Join(p, "bolt.db"), 0600, nil)
}

func closeDB(db *bolt.DB) error {
	return db.Close()
}

func put(db *bolt.DB, key, value []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("posts"))
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
}

func get(db *bolt.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("posts"))
		value = b.Get(key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func init() {
	fmt.Println("this is bolt db")
}
