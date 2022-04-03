package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2/utils"
	"github.com/thylong/bouine/pkg/backend"
)

var testStore *Storage

func TestMain(m *testing.M) {
	fmt.Println("Initialize Raft (single node leader)")
	backend.BadgerFilesCleanup("/tmp/")
	backend.BoltdbFilesCleanup("/tmp/")
	testStore = New(Config{RaftID: "test", RaftDir: "/tmp/", RaftBootstrap: true, RaftAddress: "localhost:50051"})
	// wait for leader election to be over
	time.Sleep(2 * time.Second)

	exitVal := m.Run()

	fmt.Println("Cleanup Badger & Bolt files")
	defer backend.BadgerFilesCleanup("/tmp/")
	defer backend.BoltdbFilesCleanup("/tmp/")
	os.Exit(exitVal)
}

func Test_Badger_Set(t *testing.T) {
	var (
		key = "john"
		val = []byte("doe")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, nil, err)
}

func Test_Badger_Set_Override(t *testing.T) {
	var (
		key = "john"
		val = []byte("doe")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, nil, err)

	err = testStore.Set(key, val, 0)
	utils.AssertEqual(t, nil, err)
}

func Test_Badger_Set_Empty_Key(t *testing.T) {
	var (
		key = ""
		val = []byte("doe")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, ErrEmptyKey, err)
}

func Test_Badger_Set_Empty_Value(t *testing.T) {
	var (
		key = "john"
		val = []byte("")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, ErrEmptyVal, err)
}

func Test_Badger_Get(t *testing.T) {
	var (
		key = "john"
		val = []byte("doe")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, nil, err)

	time.Sleep(150 * time.Millisecond)

	result, err := testStore.Get(key)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, val, result)
}

func Test_Badger_Get_Empty_Key(t *testing.T) {
	result, err := testStore.Get("")
	utils.AssertEqual(t, ErrEmptyKey, err)
	utils.AssertEqual(t, true, len(result) == 0)
}

func Test_Badger_Set_Expiration(t *testing.T) {
	var (
		key = "john"
		val = []byte("doe")
		exp = 1 * time.Second
	)

	err := testStore.Set(key, val, exp)
	utils.AssertEqual(t, nil, err)

	time.Sleep(1100 * time.Millisecond)
}

func Test_Badger_Get_Expired(t *testing.T) {
	var (
		key = "john"
	)

	result, err := testStore.Get(key)
	utils.AssertEqual(t, ErrKeyNotFound, err)
	utils.AssertEqual(t, true, len(result) == 0)
}

func Test_Badger_Get_NotExist(t *testing.T) {
	result, err := testStore.Get("notexist")
	utils.AssertEqual(t, ErrKeyNotFound, err)
	utils.AssertEqual(t, true, len(result) == 0)
}

func Test_Badger_Delete(t *testing.T) {
	var (
		key = "john"
		val = []byte("doe")
	)

	err := testStore.Set(key, val, 0)
	utils.AssertEqual(t, nil, err)

	err = testStore.Delete(key)
	utils.AssertEqual(t, nil, err)

	result, err := testStore.Get(key)
	utils.AssertEqual(t, ErrKeyNotFound, err)
	utils.AssertEqual(t, true, len(result) == 0)
}

func Test_Badger_Delete_Empty_Key(t *testing.T) {
	err := testStore.Delete("")
	utils.AssertEqual(t, ErrEmptyKey, err)
}

func Test_Badger_Reset(t *testing.T) {
	var (
		val = []byte("doe")
	)

	err := testStore.Set("john1", val, 0)
	utils.AssertEqual(t, nil, err)

	err = testStore.Set("john2", val, 0)
	utils.AssertEqual(t, nil, err)

	err = testStore.Reset()
	utils.AssertEqual(t, nil, err)

	result, err := testStore.Get("john1")
	utils.AssertEqual(t, ErrKeyNotFound, err)
	utils.AssertEqual(t, true, len(result) == 0)

	result, err = testStore.Get("john2")
	utils.AssertEqual(t, ErrKeyNotFound, err)
	utils.AssertEqual(t, true, len(result) == 0)
}

func Test_Badger_Close(t *testing.T) {
	utils.AssertEqual(t, nil, testStore.Close())
}
