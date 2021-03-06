package dpn_test

// Common functions for dpn_test package

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/dpn"
	"github.com/crowdmob/goamz/aws"
	"github.com/satori/go.uuid"
	"testing"
	"time"
)

var config bagman.Config
var fluctusUrl string = "http://localhost:3000"

func awsEnvAvailable() (envVarsOk bool) {
	_, err := aws.EnvAuth()
	return err == nil
}

func loadConfig(t *testing.T, configPath string) (*dpn.DPNConfig) {
	if dpnConfig != nil {
		return dpnConfig
	}
	var err error
	dpnConfig, err = dpn.LoadConfig(configPath, "test")
	if err != nil {
		t.Errorf("Error loading %s: %v\n", configPath, err)
		return nil
	}

	// Turn this off to suppress tons of debug messages.
	// dpnConfig.LogToStderr = false

	return dpnConfig
}


// Creates a DPN bag.
func MakeBag() (*dpn.DPNBag) {
	youyoueyedee := uuid.NewV4()
	randChars := youyoueyedee.String()[0:8]
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.DPNBag {
		UUID: youyoueyedee.String(),
		Interpretive: []string{},
		Rights: []string{},
		ReplicatingNodes: []string{},
		Fixities: &dpn.DPNFixity{
			Sha256: randChars,
		},
		LocalId: fmt.Sprintf("GO-TEST-BAG-%s", youyoueyedee.String()),
		Size: 12345678,
		FirstVersionUUID: youyoueyedee.String(),
		Version: 1,
		BagType: "D",
		IngestNode: "aptrust",
		AdminNode: "aptrust",
		Member: "9a000000-0000-4000-a000-000000000002", // Faber College
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// Creates a DPN replication transfer object.
func MakeXferRequest(fromNode, toNode, bagUuid string) (*dpn.DPNReplicationTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.DPNReplicationTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		BagId: bagUuid,
		ReplicationId: uuid.NewV4().String(),
		FixityAlgorithm: "sha256",
		FixityNonce: nil,
		FixityValue: nil,
		FixityAccept: nil,
		BagValid: nil,
		Status: "requested",
		Protocol: "rsync",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// Creates a DPN restore transfer object.
func MakeRestoreRequest(fromNode, toNode, bagUuid string) (*dpn.DPNRestoreTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.DPNRestoreTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		BagId: bagUuid,
		RestoreId: uuid.NewV4().String(),
		Status: "requested",
		Protocol: "rsync",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// This is the struct returned by AddRecords, so the caller can
// know which records were created.
type Mock struct {
	DPNSync   *dpn.DPNSync
	Bags      []*dpn.DPNBag
	Xfers     []*dpn.DPNReplicationTransfer
	Restores  []*dpn.DPNRestoreTransfer
}

func NewMock(dpnSync *dpn.DPNSync) *Mock {
	return &Mock{
		DPNSync: dpnSync,
	}
}

// Creates bags, transfer requests and restore requests
// at the specified nodes.
func (mock *Mock)AddRecordsToNodes(nodeNamespaces []string, count int) (err error) {
	for _, node := range nodeNamespaces {
		err = mock.AddRecordsToNode(node, count)
		if err != nil {
			return err
		}
	}
	return nil
}

// Create some bags, transfer requests and restore requests
// at the specified node.
func (mock *Mock)AddRecordsToNode(nodeNamespace string, count int) (err error) {
	allNodes, err := mock.DPNSync.GetAllNodes()
	if err != nil {
		return fmt.Errorf("While adding records, " +
			"can't get list of nodes: %v", err)
	}
	client := mock.DPNSync.RemoteClients[nodeNamespace]
	if nodeNamespace == mock.DPNSync.LocalNodeName() {
		client = mock.DPNSync.LocalClient
	}
	if client == nil {
		return fmt.Errorf("No client available for node %s", nodeNamespace)
	}
	for i := 0; i < count; i++ {
		// Create bags...
		bag := MakeBag()
		bag.IngestNode = nodeNamespace
		bag.AdminNode = nodeNamespace
		_, err = client.DPNBagCreate(bag)
		if err != nil {
			return err
		}
		mock.Bags = append(mock.Bags, bag)

		for _, otherNode := range allNodes {
			// Don't create transfers to the current node
			if otherNode.Namespace == nodeNamespace {
				continue
			}
			// Create replication transfers
			xfer := MakeXferRequest(nodeNamespace,
				otherNode.Namespace, bag.UUID)
			_, err = client.ReplicationTransferCreate(xfer)
			if err != nil {
				return err
			}
			mock.Xfers = append(mock.Xfers, xfer)

			// Create restore transfers
			restore := MakeRestoreRequest(otherNode.Namespace,
				nodeNamespace, bag.UUID)
			_, err = client.RestoreTransferCreate(restore)
			if err != nil {
				return err
			}
			mock.Restores = append(mock.Restores, restore)
		}
	}
	return nil
}
