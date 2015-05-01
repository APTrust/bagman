package dpn

/*
restobjects.go includes a number of structures used in communicating
with the DPN REST service. More info about the DPN REST service is
available at:

https://github.com/dpn-admin/DPN-REST-Wiki
https://github.com/dpn-admin/DPN-REST
*/

import (
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
	Storage              *DPNStorage  `json:"storage"`
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
	Algorithm string                   `json:"algorithm"`

	// The fixity digest, as a hex-encoded string.
	Digest string                      `json:"digest"`

	// The datetime at which this digest was calculated.
	CreatedAt time.Time                `json:"created_at"`
}

// DPNBag represents a Bag object in the DPN REST service.
// Like all of the DPN REST objects, it contains metadata only.
type DPNBag struct {

	// UUID is the unique identifier for a bag
	UUID               string               `json:"uuid"`

	// LocalId is the depositor's local identifier for a bag.
	LocalId            string               `json:"local_id"`

	// Size is the size, in bytes of the bag.
	Size               uint64               `json:"size"`

	// FirstVersionUUID is the UUID of the first version
	// of this bag.
	FirstVersionUUID   string               `json:"first_version_uuid"`

	// Version is the version or revision number of the bag. Starts at 1.
	Version            uint32               `json:"version"`

	// IngestNode is the node that first ingested or produced the bag.
	IngestNode       string                 `json:"ingest_node"`

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

	// TODO: Check REST service's serialization of Fixity values!
	//
	// Hmm... we seem to have two implementations of how fixities
	// are serialized. In one, we get this:
	// [
	//   { "algorithm": "sha256",
	//     "digest": "tums-for-digestion",
	//     "created_at": "2015-05-01T12:32:17.703526Z"
    //   }
	// ]
	//
	// In the other, we get this:
	// [{ "sha256": "1fdc62a", "sha512": "9f8d23ae" }]
	//
	// If the second is correct, we'll need to switch
	// Fixities to this:
	//
	// Fixities           []map[string]string  `json:"fixities"`
	Fixities           []*DPNFixity         `json:"fixities"`

	// CreatedAt is when this record was created.
	CreatedAt          time.Time            `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt          time.Time            `json:"updated_at"`
}

type DPNReplicationTransfer struct {
	FromNode        string       `json:"from_node"`
	ToNode          string       `json:"to_node"`
	UUID            string       `json:"uuid"`
	ReplicationId   string       `json:"replication_id"`
	FixityAlgorithm string       `json:"fixity_algorithm"`
	FixityNonce     string       `json:"fixity_nonce"`
	FixityValue     string       `json:"fixity_value"`
	FixityAccept    bool         `json:"fixity_accept"`
	BagValid        bool         `json:"bag_valid"`
	Status          string       `json:"status"`
	Protocol        string       `json:"protocol"`
	Link            string       `json:"link"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

type DPNRestoreTransfer struct {
	RestoreId       string       `json:"restore_id"`
	FromNode        string       `json:"from_node"`
	ToNode          string       `json:"to_node"`
	UUID            string       `json:"uuid"`
	Status          string       `json:"status"`
	Protocol        string       `json:"protocol"`
	Link            string       `json:"link"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}
