package dpn

/*
restobjects.go includes a number of structures used in communicating
with the DPN REST service. This file defines only those DPN REST structures
related to storing bags and creating and fulfilling replication requests.
*/

include (
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
	UUID               string

	// LocalId is the depositor's local identifier for a bag.
	LocalId            string

	// Size is the size, in bytes of the bag.
	Size               uint64

	// FirstVersionUUID is the UUID of the first version
	// of this bag.
	FirstVersionUUID   string

	// Version is the version or revision number of the bag. Starts at 1.
	Version            uint32

	// OriginalNode is the node that first ingested or produced the bag.
	OriginalNode       string

	// BagType is one of 'D' (Data), 'R' (Rights) or 'I' (Interpretive)
	BagType            string

	// Rights is a list of UUIDs of rights objects for this bag.
	Rights             []string

	// Interpretive is a list of UUIDs of interpretive objects for this bag.
	Interpretive       []string

	// ReplicatingNodes is a list of nodes that have replicated this bag.
	Rights             []string

	// AdminNode is the authoritative node for this bag. If various nodes
	// have conflicting registry info for this bag, the admin node wins.
	// The admin node also has some authority in restoring and (if its ever
	// possible) deleting bags.
	AdminNode          string
}

type DPNReplicationTransfer struct {

}

type DPNRestoreTransfer struct {

}
