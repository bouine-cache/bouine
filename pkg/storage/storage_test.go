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

func createStorage(raftDir, raftID, raftAddress string, raftBootstrap bool) *Storage {
	_ = os.MkdirAll(raftDir, 0770)
	// if err != nil {
	// 	panic(err)
	// }

	fmt.Println("Initialize Raft (single node leader)")
	// Delete potential orphans from previous test runs
	backend.BadgerFilesCleanup(raftDir)
	backend.BoltdbFilesCleanup(raftDir)

	return New(Config{RaftID: raftID, RaftDir: raftDir, RaftBootstrap: raftBootstrap, RaftAddress: raftAddress})
}

func TestMain(m *testing.M) {
	testStore = createStorage("/tmp/bouine/storage", "testMain", "localhost:50051", true)
	defer backend.BadgerFilesCleanup("/tmp/bouine/storage")
	defer backend.BoltdbFilesCleanup("/tmp/bouine/storage")
	// wait for leader election to be over
	time.Sleep(2 * time.Second)

	exitVal := m.Run()

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

// func Test_Storage_forwardToLeader(t *testing.T) {
// 	firstRaftDir := "/tmp/bouine/storage_forwardToLeader_1"
// 	secondRaftDir := "/tmp/bouine/storage_forwardToLeader_2"
// 	backend.BadgerFilesCleanup(firstRaftDir)
// 	backend.BoltdbFilesCleanup(secondRaftDir)
// 	defer backend.BadgerFilesCleanup(firstRaftDir)
// 	defer backend.BoltdbFilesCleanup(secondRaftDir)

// 	// Create two Raft Nodes cluster
// 	firstTestStore := createStorage(firstRaftDir, "forwardToLeader1", "localhost:50061", true)
// 	secondTestStore := createStorage(secondRaftDir, "forwardToLeader2", "localhost:50062", false)
// 	go func() {
// 		if err := firstTestStore.ListengRPCServer("localhost:50061"); err != nil {
// 			fmt.Fprintf(os.Stderr, "%s\n", err)
// 			os.Exit(1)
// 		}
// 	}()
// 	go func() {
// 		if err := firstTestStore.ListengRPCServer("localhost:50062"); err != nil {
// 			fmt.Fprintf(os.Stderr, "%s\n", err)
// 			os.Exit(1)
// 		}
// 	}()

// 	// wait for leader elections to be over
// 	time.Sleep(2 * time.Second)

// 	// register Follower
// 	timeout, _ := time.ParseDuration("2s")
// 	f := firstTestStore.r.AddVoter("forwardToLeader2", "localhost:50062", 0, timeout)
// 	if err := f.Error(); err != nil {
// 		t.Errorf("Test_Storage_forwardToLeader failed: %s", err)
// 	}
// 	time.Sleep(4 * time.Second)

// 	// write to leader (require to have an up to date Leader() value)
// 	err := firstTestStore.Set("john", []byte("doe"), 10000000000)
// 	utils.AssertEqual(t, nil, err)

// 	fmt.Println(firstTestStore.r.State().String())
// 	fmt.Println(secondTestStore.r.State().String())
// 	os.Exit(1)

// 	time.Sleep(1 * time.Second)

// 	cf := secondTestStore.r.GetConfiguration()
// 	if err := cf.Error(); err != nil {
// 		t.Errorf("Test_Storage_forwardToLeader failed: %s", err)
// 	}

// 	for k, v := range cf.Configuration().Servers {
// 		fmt.Printf("SERVER%d: %#v\n", k, v)
// 	}

// 	// forward from second Raft node to cluster
// 	err = secondTestStore.forwardToLeader("john", []byte("doe"), timeout)
// 	if err != nil {
// 		t.Errorf("unexpected error: %s", err)
// 	}
// 	// wait for log entry to be committed on the FSM
// 	time.Sleep(1 * time.Second)
// }
