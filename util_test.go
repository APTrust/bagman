package bagman_test

import (
    "testing"
    "os"
    "path/filepath"
    "github.com/APTrust/bagman"
)

func TestBagmanHome(t *testing.T) {
    bagmanHome := os.Getenv("BAGMAN_HOME")
    goHome := os.Getenv("GO_HOME")
    defer os.Setenv("BAGMAN_HOME", bagmanHome)
    defer os.Setenv("GO_HOME", goHome)

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

    // Otherwise, should use GO_HOME
    os.Setenv("GO_HOME", "/go_home")
    bagmanHome, err = bagman.BagmanHome()
    if err != nil {
        t.Error(err)
    }
    if bagmanHome != "/go_home/src/github.com/APTrust/bagman" {
        t.Errorf("BagmanHome returned '%s', expected '%s'",
            bagmanHome,
            "/go_home")
    }
    os.Setenv("GO_HOME", "")

    // Without BAGMAN_HOME and GO_HOME, we should get an error
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
