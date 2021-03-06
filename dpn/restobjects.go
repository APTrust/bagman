package dpn

/*
restobjects.go includes a number of structures used in communicating
with the DPN REST service. More info about the DPN REST service is
available at:

https://github.com/dpn-admin/DPN-REST-Wiki
https://github.com/dpn-admin/DPN-REST
*/

import (
	"math/rand"
	"time"
)

type DPNNode struct {
	Name                 string       `json:"name"`
	Namespace            string       `json:"namespace"`
	APIRoot              string       `json:"api_root"`
	SSHPubKey            string       `json:"ssh_pubkey"`
	ReplicateFrom        []string     `json:"replicate_from"`
	ReplicateTo          []string     `json:"replicate_to"`
	RestoreFrom          []string     `json:"restore_from"`
	RestoreTo            []string     `json:"restore_to"`
	Protocols            []string     `json:"protocols"`
	FixityAlgorithms     []string     `json:"fixity_algorithms"`
	CreatedAt            time.Time    `json:"created_at"`
	UpdatedAt            time.Time    `json:"updated_at"`
	LastPullDate         time.Time    `json:"last_pull_date"`
	Storage              *DPNStorage  `json:"storage"`
}

// This randomly chooses nodes for replication, returning
// a slice of strings. Each string is the namespace of a node
// we should replicate to. This may return fewer nodes than
// you specified in the howMany param if this node replicates
// to fewer nodes.
//
// We may have to revisit this in the future, if DPN specifies
// logic for how to choose remote nodes. For now, we can choose
// any node, because they are all geographically diverse and
// all use different storage backends.
func (node *DPNNode) ChooseNodesForReplication(howMany int) ([]string) {
	selectedNodes := make([]string, 0)
	if howMany >= len(node.ReplicateTo) {
		for _, namespace := range node.ReplicateTo {
			selectedNodes = append(selectedNodes, namespace)
		}
	} else {
		nodeMap := make(map[string]int)
		for len(selectedNodes) < howMany {
			randInt := rand.Intn(len(node.ReplicateTo))
			namespace := node.ReplicateTo[randInt]
			if _, alreadyAdded := nodeMap[namespace]; !alreadyAdded {
				selectedNodes = append(selectedNodes, namespace)
				nodeMap[namespace] = randInt
			}
		}
	}
	return selectedNodes
}

type DPNStorage struct {
	Region               string        `json:"region"`
	Type                 string        `json:"type"`
}

// DPNFixity represents a checksum for a bag in the DPN REST
// service.
type DPNFixity struct {

	// The algorithm used to check the fixity. Usually 'sha256',
	// but others may be valid in the future.
	Sha256               string       `json:"sha256"`

}

// DPNMember describes an institution or depositor that owns
// a bag.
type DPNMember struct {

	// UUID is the unique identifier for a member
	UUID               string               `json:"uuid"`

	// Name is the member's name
	Name               string               `json:"name"`

	// Email is the member's email address
	Email              string               `json:"email"`

	// CreatedAt is when this record was created.
	CreatedAt          time.Time            `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt          time.Time            `json:"updated_at"`

}

// DPNBag represents a Bag object in the DPN REST service.
// Like all of the DPN REST objects, it contains metadata only.
type DPNBag struct {

	// UUID is the unique identifier for a bag
	UUID               string               `json:"uuid"`

	// LocalId is the depositor's local identifier for a bag.
	LocalId            string               `json:"local_id"`

	// Member is the UUID of the member who deposited this bag.
	Member             string               `json:"member"`

	// Size is the size, in bytes of the bag.
	Size               uint64               `json:"size"`

	// FirstVersionUUID is the UUID of the first version
	// of this bag.
	FirstVersionUUID   string               `json:"first_version_uuid"`

	// Version is the version or revision number of the bag. Starts at 1.
	Version            uint32               `json:"version"`

	// IngestNode is the node that first ingested or produced the bag.
	IngestNode         string               `json:"ingest_node"`

	// AdminNode is the authoritative node for this bag. If various nodes
	// have conflicting registry info for this bag, the admin node wins.
	// The admin node also has some authority in restoring and (if its ever
	// possible) deleting bags.
	AdminNode          string               `json:"admin_node"`

	// BagType is one of 'D' (Data), 'R' (Rights) or 'I' (Interpretive)
	BagType            string               `json:"bag_type"`

	// Rights is a list of UUIDs of rights objects for this bag.
	Rights             []string             `json:"rights"`

	// Interpretive is a list of UUIDs of interpretive objects for this bag.
	Interpretive       []string             `json:"interpretive"`

	// ReplicatingNodes is a list of one more nodes that has stored
	// copies of this bag. The items in the list are node namespaces,
	// which are strings. E.g. ['aptrust', 'chron', 'tdr']
	ReplicatingNodes   []string             `json:"replicating_nodes"`

	// Fixities are the checksum/fixity values for this bag.
	Fixities           *DPNFixity           `json:"fixities"`

	// CreatedAt is when this record was created.
	CreatedAt          time.Time            `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt          time.Time            `json:"updated_at"`
}

type DPNReplicationTransfer struct {

	// FromNode is the node where the bag is coming from.
	// The FromNode initiates the replication request.
	FromNode        string       `json:"from_node"`

	// ToNode is the node the bag is being transfered to
	ToNode          string       `json:"to_node"`

	// Bag is the UUID of the bag to be replicated
	BagId           string       `json:"uuid"`

	// ReplicationId is a unique id for this replication request.
	// It's a UUID in string format.
	ReplicationId   string       `json:"replication_id"`

	// FixityAlgorithm is the algorithm used to calculate the fixity digest.
	FixityAlgorithm string       `json:"fixity_algorithm"`

	// FixityNonce is an optional nonce used to calculate the fixity digest.
	FixityNonce     *string      `json:"fixity_nonce"`

	// FixityValue is the fixity value calculated by the ToNode after
	// it receives the bag. This will be null/empty until the replicating
	// node sends the info back to the FromNode.
	FixityValue     *string      `json:"fixity_value"`

	// FixityAccept describes whether the FromNode accepts the fixity
	// value calculated by the ToNode. This is a nullable boolean,
	// so it has to be a pointer.
	FixityAccept    *bool        `json:"fixity_accept"`

	// BagValid is a value set by the ToNode to indicate whether
	// the bag it received was valid. This is a nullable boolean,
	// so it has to be a pointer.
	BagValid        *bool        `json:"bag_valid"`

	// Status is the status of the request, which can be any of:
	//
	// "requested"  - The FromNode has requested this transfer.
	//                This means the transfer is new, and no
	//                action has been taken yet.
	// "rejected"   - Set by the ToNode when it will not or cannot
	//                accept this transfer. (Usually due to disk space.)
	// "received"   - Set by the ToNode to indicate it has received the
	//                the bag.
	// "confirmed"  - Set by the FromNode after the bag has been confirmed
	//                valid, the fixity value has been approved, and the bag
	//                has been stored by the ToNode.
	// "stored"     - Set by the ToNode after the bag has been copied to
	//                long-term storage.
	// "cancelled"  - Can be set by either node for any reason. No further
	//                processing should occur on a cancelled request.
	Status          string       `json:"status"`

	// Protocol is the network protocol used to transfer the bag.
	// At launch, the only valid value for this is 'R' for rsync.
	Protocol        string       `json:"protocol"`

	// Link is a URL that the ToNode can use to copy the bag from the
	// FromNode. This value is set by the FromNode.
	Link            string       `json:"link"`

	// CreatedAt is the datetime when this record was created.
	CreatedAt       time.Time    `json:"created_at"`

	// UpdatedAt is the datetime when this record was last updated.
	UpdatedAt       time.Time    `json:"updated_at"`
}

type DPNRestoreTransfer struct {

	// RestoreId is a unique id for this restoration request.
	RestoreId       string       `json:"restore_id"`

	// FromNode is the node from which the bag should be restored.
	FromNode        string       `json:"from_node"`

	// ToNode is the node to which the bag should be restored.
	// The ToNode initiates a restoration request.
	ToNode          string       `json:"to_node"`

	// Bag is the unique identifier of the bag to be restored.
	BagId           string       `json:"uuid"`

	// Status is the status of the restoration operation. It can
	// have any of the following values:
	//
	// "requested" - Default status used when record first created.
	// "accepted"  - Indicates the FromNode has accepted the request to
	//               restore the bag.
	// "rejected"  - Set by the FromNode if it cannot or will not restore
	//               the bag.
	// "prepared"  - Set by the FromNode when the content has been restored
	//               locally and staged for transfer back to the to_node.
	// "finished"  - Set by the ToNode after it has retrieved the restored
	//               bag from the FromNode.
	// "cancelled" - Set by either node to indicate the restore operation
	//               was cancelled.
	Status          string       `json:"status"`

	// Protocol is the network protocol used to transfer the bag.
	// At launch, the only valid value for this is 'R' for rsync.
	Protocol        string       `json:"protocol"`

	// Link is a URL that the ToNode can use to copy the bag from the
	// FromNode. This value is set by the FromNode.
	Link            string       `json:"link"`

	// CreatedAt is the datetime when this record was created.
	CreatedAt       time.Time    `json:"created_at"`

	// UpdatedAt is the datetime when this record was last updated.
	UpdatedAt       time.Time    `json:"updated_at"`
}
