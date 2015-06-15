package dpn_test

import (
	"github.com/APTrust/bagman/dpn"
	"testing"
)

func TestChooseNodesForReplication(t *testing.T) {
	nodelist := []string{
		"node1", "node2", "node3",
		"node4", "node5", "node6",
	}
	node := &dpn.DPNNode{
		ReplicateTo: nodelist,
	}
	ints := []int { 1,2,3,4,5,6 }
	for _, num := range ints {
		replicatingNodes := node.ChooseNodesForReplication(num)
		if len(replicatingNodes) != num {
			t.Errorf("Expected %d nodes, got %d", num, len(replicatingNodes))
		}
		unique, duplicate := assertUnique(replicatingNodes)
		if unique == false {
			t.Errorf("Node %s appears more than once in replication list", duplicate)
		}
	}

	// Ask for more nodes than we have in our list
	num := len(nodelist) + 1
	replicatingNodes := node.ChooseNodesForReplication(num)
	if len(replicatingNodes) != len(nodelist) {
		t.Errorf("Expected %d nodes, got %d", len(nodelist), len(replicatingNodes))
	}
	unique, duplicate := assertUnique(replicatingNodes)
	if unique == false {
		t.Errorf("Node %s appears more than once in replication list", duplicate)
	}
}

func assertUnique(list []string) (bool, string) {
	count := make(map[string]int)
	for _, val := range list {
		if _, hasCount := count[val]; !hasCount {
			// We haven't counted this value yet.
			count[val] = 1
		} else {
			// We have already counted this value,
			// and now we're seeing it again.
			// List items are not unique.
			return false, val
		}
	}
	return true, ""
}
