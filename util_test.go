package bagman_test

import (
	"github.com/APTrust/bagman"
	"os"
	"path/filepath"
	"testing"
)

func TestBagmanHome(t *testing.T) {
	bagmanHome := os.Getenv("BAGMAN_HOME")
	goHome := os.Getenv("GOPATH")
	defer os.Setenv("BAGMAN_HOME", bagmanHome)
	defer os.Setenv("GOPATH", goHome)

	// Should use BAGMAN_HOME, if it's set...
	os.Setenv("BAGMAN_HOME", "/bagman_home")
	bagmanHome, err := bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/bagman_home" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/bagman_home")
	}
	os.Setenv("BAGMAN_HOME", "")

	// Otherwise, should use GOPATH
	os.Setenv("GOPATH", "/go_home")
	bagmanHome, err = bagman.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/go_home/src/github.com/APTrust/bagman" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/go_home")
	}
	os.Setenv("GOPATH", "")

	// Without BAGMAN_HOME and GOPATH, we should get an error
	bagmanHome, err = bagman.BagmanHome()
	if err == nil {
		t.Error("BagmanHome should have an thrown exception.")
	}
}

func TestLoadRelativeFile(t *testing.T) {
	path := filepath.Join("testdata", "result_good.json")
	data, err := bagman.LoadRelativeFile(path)
	if err != nil {
		t.Error(err)
	}
	if data == nil || len(data) == 0 {
		t.Errorf("Read no data out of file '%s'", path)
	}
}

func TestSyncMap(t *testing.T) {
	syncMap := bagman.NewSynchronizedMap()
	keys1 := []string{"1", "2", "3", "4", "5"}
	keys2 := []string{"6", "7", "8", "9", "10"}
	go testSyncMap(t, syncMap, keys1)
	go testSyncMap(t, syncMap, keys2)
}

func testSyncMap(t *testing.T, syncMap *bagman.SynchronizedMap, keys []string) {
	for i, key := range keys {
		syncMap.Add(key, key)
		if syncMap.HasKey(key) == false {
			t.Errorf("SyncMap should have key %s", key)
		}
		if syncMap.Get(key) != key {
			t.Errorf("SyncMap key %s has value %s, expected %s", key, syncMap.Get(key), key)
		}
		if len(syncMap.Keys()) < i {
			t.Errorf("SyncMap should have at least %d keys, but it has %d", i, len(syncMap.Keys()))
		}
		if len(syncMap.Values()) < i {
			t.Errorf("SyncMap should have at least %d values, but it has %d", i, len(syncMap.Values()))
		}
	}
}

func TestSyncMapDelete(t *testing.T) {
	syncMap := bagman.NewSynchronizedMap()
	keys := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}

	for _, key := range keys {
		syncMap.Add(key, key)
	}
	if len(syncMap.Keys()) != len(keys) {
		t.Errorf("SyncMap should have %d keys, but it has %d", len(keys), len(syncMap.Keys()))
	}
	if len(syncMap.Values()) != len(keys) {
		t.Errorf("SyncMap should have %d values, but it has %d", len(keys), len(syncMap.Values()))
	}
	for _, key := range keys {
		syncMap.Delete(key)
		if syncMap.HasKey(key) == true {
			t.Errorf("SyncMap should not have key %s", key)
		}
	}
	if len(syncMap.Keys()) != 0 {
		t.Errorf("SyncMap should have 0 keys, but it has %d", len(syncMap.Keys()))
	}
	if len(syncMap.Values()) != 0 {
		t.Errorf("SyncMap should have 0 values, but it has %d", len(syncMap.Values()))
	}
}
