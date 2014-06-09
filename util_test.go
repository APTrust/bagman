package bagman_test

import (
    "testing"
    "os"
    "github.com/APTrust/bagman"
)

func TestBagmanHome(t *testing.T) {
    bagmanHome := os.Getenv("BAGMAN_HOME")
    goHome := os.Getenv("GO_HOME")

    // Should use BAGMAN_HOME, if it's set...
    os.Setenv("BAGMAN_HOME", "/bagman_home")
    defer os.Setenv("BAGMAN_HOME", bagmanHome)
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
    defer os.Setenv("GO_HOME", goHome)
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

    bagmanHome, err = bagman.BagmanHome()
    if err == nil {
        t.Error("BagmanHome should have an thrown exception.")
    }

}
