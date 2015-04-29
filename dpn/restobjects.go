package dpn

/*
restobjects.go includes a number of structures used in communicating
with the DPN REST service. This file defines only those DPN REST structures
related to storing bags and creating and fulfilling replication requests.

More info about the DPN REST service is available at:

https://github.com/dpn-admin/DPN-REST-Wiki
https://github.com/dpn-admin/DPN-REST
*/

import (
	"time"
)

// DPNFixity represents a checksum for a bag in the DPN REST
// service.
type DPNFixity struct {

	// The algorithm used to check the fixity. Usually 'sha256',
	// but others may be valid in the future.
	Algorithm string

	// The fixity digest, as a hex-encoded string.
	Digest string

	// The datetime at which this digest was calculated.
	CreatedAt time.Time
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
	AdminNode          string               `json:"ingest_node"`

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

	// Fixities is a list of hashes. In each hash, the key is the
	// fixity algorithm and the value is the fixity digest. So you
	// might see something like this:
	// [{ "sha256": "1fdc62a", "sha512": "9f8d23ae" }]
	Fixities           []map[string]string  `json:"fixities"`

	// CreatedAt is when this record was created.
	CreatedAt          time.Time            `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt          time.Time            `json:"updated_at"`
}

type DPNReplicationTransfer struct {
	FromNode        string       `json:"from_node"`
	ToNode          string       `json:"to_node"`
	UUID            string       `json:"bag"`
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
	FromNode        string       `json:"from_node"`
	ToNode          string       `json:"to_node"`
	UUID            string       `json:"bag"`
	Status          string       `json:"status"`
	Protocol        string       `json:"protocol"`
	Link            string       `json:"link"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}
