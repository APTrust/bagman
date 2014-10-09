package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
)

func TestTagValue(t *testing.T) {
	result := &bagman.BagReadResult{}
	result.Tags = make([]bagman.Tag, 2)
	result.Tags[0] = bagman.Tag{Label: "Label One", Value: "Value One"}
	result.Tags[1] = bagman.Tag{Label: "Label Two", Value: "Value Two"}

	if result.TagValue("LABEL ONE") != "Value One" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("Label Two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("label two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if result.TagValue("Non-existent label") != "" {
		t.Error("TagValue returned wrong result.")
	}
}
