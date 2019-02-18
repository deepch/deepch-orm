package orm

import (
	"log"
	"os"
	"testing"
)

var Base *DB

func TestOpen(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		log.Panic(err)
	}
	if db == nil {
		log.Panic("db == nil")
	}
	Base = db
	log.Println("OPEN")
}
func TestInsert(t *testing.T) {
	err := Base.Insert("test", 99, []byte("deep"))
	if err != nil {
		log.Panic(err)
	}
	log.Println("INSERT")
}
func TestInsertMultiple(t *testing.T) {
	ret := make(map[interface{}][]byte)
	ret[101] = []byte{98}
	ret[102] = []byte{99}
	ret[103] = []byte{100}
	err := Base.InsertMultiple("test", ret)
	if err != nil {
		log.Panic(err)
	}
	log.Println("INSERT Multiple", ret)
}

func TestSelectMultiple(t *testing.T) {
	val, err := Base.Select("test", 101)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT FOR M", val)
	val, err = Base.Select("test", 102)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT FOR M", val)
	val, err = Base.Select("test", 103)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT FOR M", val)
}

func TestSelect(t *testing.T) {
	val, err := Base.Select("test", 99)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT", string(val))
}

func TestDelete(t *testing.T) {
	err := Base.Delete("test", []byte("1"))
	if err != nil {
		log.Panic(err)
	}
	log.Println("DELETE")
}
func TestSelectRange(t *testing.T) {
	count := 100
	for i := 0; i < count; i++ {
		err := Base.Insert("test", i, []byte{0})
		if err != nil {
			log.Panicln(err)
		}
	}
	val, err := Base.SelectRange("test", 5, 10)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT RANGE", val)
}

func TestSelectExtend(t *testing.T) {
	val, err := Base.SelectRangeExtend("test", 6, 7)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT RANGE EXTEND", val)
}

func TestDeleteRange(t *testing.T) {
	err := Base.DeleteRange("test", 5, 7)
	if err != nil {
		log.Panic(err)
	}
	log.Println("DELETE RANGE")
	val, err := Base.SelectRange("test", 5, 10)
	if err != nil {
		log.Panic(err)
	}
	log.Println("SELECT RANGE FROM DELETE", val)
}
func TestClose(t *testing.T) {
	Base.Close()
	log.Println("CLOSE")
}

func TestRm(t *testing.T) {
	err := os.Remove("test.db")
	if err != nil {
		log.Panicln(err)
	}
	log.Println("REMOVE OK")
}
