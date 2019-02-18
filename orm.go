package orm

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
)

//DB global struct
type DB struct {
	base *bolt.DB
}

//Open database
func Open(name string) (*DB, error) {
	m := DB{}
	db, err := bolt.Open(name+".db", 0600, nil)
	if err != nil {
		return nil, err
	}
	m.base = db
	return &m, nil
}

//Insert Data To Base
func (obj *DB) Insert(table string, key interface{}, val []byte) error {
	err := obj.base.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return fmt.Errorf("create table: %s", err)
		}
		err = b.Put(Assert(key), val)
		if err != nil {
			return fmt.Errorf("insert key: %s", err)
		}
		return nil
	})
	return err
}

//InsertMultiple
func (obj *DB) InsertMultiple(table string, data map[interface{}][]byte) error {
	tx, err := obj.base.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for k, v := range data {
		b, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}
		err = b.Put(Assert(k), v)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

//Update func
func (obj *DB) Update(table string, key interface{}, val []byte) error {
	err := obj.Insert(table, key, val)
	return err
}

//Select func
func (obj *DB) Select(table string, key interface{}) ([]byte, error) {
	var v []byte
	err := obj.base.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))
		if b == nil {
			return fmt.Errorf("table not found")
		}
		v = b.Get(Assert(key))
		if v == nil {
			return fmt.Errorf("key not found")
		}
		return nil
	})
	return v, err
}

//SelectRange func
func (obj *DB) SelectRange(table string, start interface{}, end interface{}) (map[int64]interface{}, error) {
	startb := Assert(start)
	endb := Assert(end)
	ret := make(map[int64]interface{})
	err := obj.base.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))
		if b == nil {
			return fmt.Errorf("table not found")
		}
		c := b.Cursor()
		for k, v := c.Seek(startb); k != nil && bytes.Compare(k, endb) <= 0; k, v = c.Next() {
			ret[int64(binary.LittleEndian.Uint32(k))] = v
		}
		return nil
	})
	return ret, err
}

//SelectRangeExtend func
func (obj *DB) SelectRangeExtend(table string, start interface{}, end interface{}) (map[int64]interface{}, error) {
	startb := Assert(start)
	endb := Assert(end)
	ret := make(map[int64]interface{})
	err := obj.base.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))
		if b == nil {
			return fmt.Errorf("table not found")
		}
		c := b.Cursor()
		var fist []byte
		var last []byte
		for k, v := c.Seek(startb); k != nil && bytes.Compare(k, endb) <= 0; k, v = c.Next() {
			if len(fist) == 0 {
				fist = k
			}
			ret[int64(binary.LittleEndian.Uint32(k))] = v
			last = k
		}
		c.Seek(last)
		k, v := c.Next()
		if k != nil {
			ret[int64(binary.LittleEndian.Uint32(k))] = v
		}
		c.Seek(startb)
		k, v = c.Prev()
		if k != nil {
			ret[int64(binary.LittleEndian.Uint32(k))] = v
		}
		return nil
	})
	return ret, err
}

//DeleteRange function
func (obj *DB) DeleteRange(table string, start interface{}, end interface{}) error {
	startb := Assert(start)
	endb := Assert(end)
	err := obj.base.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))
		if b == nil {
			return fmt.Errorf("table not found")
		}
		c := b.Cursor()
		for k, _ := c.Seek(startb); k != nil && bytes.Compare(k, endb) <= 0; k, _ = c.Next() {
			c.Delete()
		}
		return nil
	})
	return err
}

//Delete func
func (obj *DB) Delete(table string, key []byte) error {
	err := obj.base.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(table))
		if b == nil {
			return fmt.Errorf("table not found")
		}
		err := b.Delete(key)
		if err != nil {
			return fmt.Errorf("key not found %s", err)
		}
		return nil
	})
	return err
}

//Close DB and work
func (obj *DB) Close() {
	obj.base.Close()
}

//Assert func
func Assert(val interface{}) []byte {
	if f, ok := val.(int64); ok {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(f))
		return buf
	}
	if f, ok := val.(int); ok {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(f))
		return buf
	}
	if s, ok := val.(string); ok {
		return []byte(s)
	}
	b := val.([]uint8)
	return b
}
