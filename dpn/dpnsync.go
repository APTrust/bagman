package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/op/go-logging"
	"net/url"
	"time"
)

type DPNSync struct {
	LocalClient    *DPNRestClient
	RemoteClients  map[string]*DPNRestClient
	Logger         *logging.Logger
	Config         *DPNConfig
}

// SyncResult describes the result of an operation where we pull
// info about all updated bags, replication requests and restore
// requests from a remote node and copy that data into our own
// local DPN registry.
type SyncResult struct {
	// Node is the node we are pulling information from.
	RemoteNode            *DPNNode
	// Bags is a list of bags successfully synched.
	Bags                  []*DPNBag
	// ReplicationTransfers successfully synched.
	ReplicationTransfers  []*DPNReplicationTransfer
	// RestoreTransfers successfully synched.
	RestoreTransfers      []*DPNRestoreTransfer
	// BagSyncError contains the error (if any) that occurred
	// during the bag sync process. The first error will stop
	// the synching of all subsquent bags.
	BagSyncError          error
	// ReplicationSyncError contains the error (if any) that occurred
	// during the synching of Replication Transfers. The first error
	// will stop the synching of all subsquent replication requests.
	ReplicationSyncError  error
	// RestoreSyncError contains the error (if any) that occurred
	// during the synching of Restore Transfers. The first error
	// will stop the synching of all subsquent restore requests.
	RestoreSyncError      error
}

func (syncResult *SyncResult) HasSyncErrors() (bool) {
	return (syncResult.BagSyncError != nil ||
		syncResult.ReplicationSyncError != nil ||
		syncResult.RestoreSyncError != nil)
}

func NewDPNSync(config *DPNConfig) (*DPNSync, error) {
	logger := initLogger(config)
	localClient, err := NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		config.LocalNode,
		config,
		logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := GetRemoteClients(localClient, config, logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating remote DPN REST client: %v", err)
	}
	sync := DPNSync{
		LocalClient: localClient,
		RemoteClients: remoteClients,
		Logger: logger,
		Config: config,
	}
	return &sync, nil
}

func initLogger(config *DPNConfig) (*logging.Logger) {
	// bagman has a nice InitLogger function, but it
	// needs a bagman.Config object, which does a lot
	// of handy internal work. Create a throw-away
	// bagman config so we can get our logger.
	bagmanConfig := bagman.Config{
		LogDirectory: config.LogDirectory,
		LogLevel: config.LogLevel,
		LogToStderr: config.LogToStderr,
	}
	return bagman.InitLogger(bagmanConfig)
}

// Returns a list of all the nodes that our node knows about.
func (dpnSync *DPNSync) GetAllNodes()([]*DPNNode, error) {
	result, err := dpnSync.LocalClient.DPNNodeListGet(nil)
	if err != nil {
		return nil, err
	}
	return result.Results, nil
}

// Sync all bags, replication requests and restore requests from
// the specified remote node. Note that this is a pull-only sync.
// We are not writing any data to other nodes, just reading what
// they have and updating our own registry with their info.
func (dpnSync *DPNSync) SyncEverythingFromNode(remoteNode *DPNNode) (*SyncResult) {
	syncResult := &SyncResult {
		RemoteNode: remoteNode,
	}

	bags, err := dpnSync.SyncBags(remoteNode)
	syncResult.Bags = bags
	syncResult.BagSyncError = err

	replXfers, err := dpnSync.SyncReplicationRequests(remoteNode)
	syncResult.ReplicationTransfers = replXfers
	syncResult.ReplicationSyncError = err

	restoreXfers, err := dpnSync.SyncRestoreRequests(remoteNode)
	syncResult.RestoreTransfers = restoreXfers
	syncResult.RestoreSyncError = err

	return syncResult
}

// Syncs bags from the specified node to our own local DPN registry
// if the bags match these critieria:
//
// 1. The node we are querying is the admin node for the bag.
// 2. The bag was updated since the last time we queried the node.
//
// Returns a list of the bags that were successfully updated.
// Even on error, this may still return a list with whatever bags
// were updated before the error occurred.
func (dpnSync *DPNSync) SyncBags(remoteNode *DPNNode) ([]*DPNBag, error) {
	pageNumber := 1
	bagsProcessed := make([]*DPNBag, 0)
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping bag sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return bagsProcessed, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of bags from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getBags(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return bagsProcessed, err
		}
		dpnSync.Logger.Debug("Got %d bags from %s", len(result.Results), remoteNode.Namespace)
		processed, err := dpnSync.syncBags(result.Results)
		if err != nil {
			return bagsProcessed, err
		}
		bagsProcessed = append(bagsProcessed, processed...)
		if result.Next == nil || *result.Next == "" {
			dpnSync.Logger.Debug("No more bags to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Processed %d bags from remote node %s",
		len(bagsProcessed), remoteNode.Namespace)
	return bagsProcessed, nil
}

func (dpnSync *DPNSync) syncBags(bags []*DPNBag) ([]*DPNBag, error) {
	bagsProcessed := make([]*DPNBag, 0)
	for _, bag := range(bags) {
		dpnSync.Logger.Debug("Processing bag %s from %s", bag.UUID, bag.AdminNode)
		existingBag, _ := dpnSync.LocalClient.DPNBagGet(bag.UUID)
		var err error
		var processedBag *DPNBag
		if existingBag != nil {
			if !existingBag.UpdatedAt.Before(bag.UpdatedAt) {
				dpnSync.Logger.Debug("Not updating bag %s, because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s", bag.UUID,
					bag.UpdatedAt, existingBag.UpdatedAt)
				continue
			} else {
				dpnSync.Logger.Debug("Bag %s exists... updating", bag.UUID)
				processedBag, err = dpnSync.LocalClient.DPNBagUpdate(bag)
			}
		} else {  // New bag
			dpnSync.Logger.Debug("Bag %s not in local registry... creating", bag.UUID)
			processedBag, err = dpnSync.LocalClient.DPNBagCreate(bag)
		}
		if err != nil {
			dpnSync.Logger.Debug("Oops! Bag %s: %v", bag.UUID, err)
			return bagsProcessed, err
		}
		bagsProcessed = append(bagsProcessed, processedBag)
	}
	return bagsProcessed, nil
}

func (dpnSync *DPNSync) getBags(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*BagListResult, error) {
	// We want to get all bags updated since the last time we pulled
	// from this node, and only those bags for which the node we're
	// querying is the admin node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("admin_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNBagListGet(&params)
}

func (dpnSync *DPNSync) SyncReplicationRequests(remoteNode *DPNNode) ([]*DPNReplicationTransfer, error) {
	xfersProcessed := make([]*DPNReplicationTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping replication sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersProcessed, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of replication requests from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getReplicationRequests(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return xfersProcessed, err
		}
		dpnSync.Logger.Debug("Got %d replication requests from %s", len(result.Results), remoteNode.Namespace)
		updated, err := dpnSync.syncReplicationRequests(result.Results)
		if err != nil {
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updated...)
		if result.Next == nil || *result.Next == "" {
			dpnSync.Logger.Debug("No more replication requests to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Processed %d replication requests from node %s",
		len(xfersProcessed), remoteNode.Namespace)
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) syncReplicationRequests(xfers []*DPNReplicationTransfer) ([]*DPNReplicationTransfer, error) {
	xfersProcessed := make([]*DPNReplicationTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Logger.Debug("Processing transfer %s in local registry", xfer.ReplicationId)
		existingXfer, _ := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
		var err error
		var updatedXfer *DPNReplicationTransfer
		if existingXfer != nil {
			if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
				dpnSync.Logger.Debug("Not updating replication request %s, because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s", xfer.ReplicationId,
					xfer.UpdatedAt, existingXfer.UpdatedAt)
			} else {
				dpnSync.Logger.Debug("Replication request %s exists... updating", xfer.ReplicationId)
				updatedXfer, err = dpnSync.LocalClient.ReplicationTransferUpdate(xfer)
			}
		} else {
			dpnSync.Logger.Debug("Replication request %s not in local registry... creating", xfer.ReplicationId)
			updatedXfer, err = dpnSync.LocalClient.ReplicationTransferCreate(xfer)
		}
		if err != nil {
			dpnSync.Logger.Debug("Oops! Replication request %s: %v", xfer.ReplicationId, err)
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updatedXfer)
	}
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) getReplicationRequests(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*ReplicationListResult, error) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the from_node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("from_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNReplicationListGet(&params)
}


func (dpnSync *DPNSync) SyncRestoreRequests(remoteNode *DPNNode) ([]*DPNRestoreTransfer, error) {
	xfersProcessed := make([]*DPNRestoreTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Logger.Error("Skipping restore sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersProcessed, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Logger.Debug("Getting page %d of restore requests from %s", pageNumber, remoteNode.Namespace)
		result, err := dpnSync.getRestoreRequests(remoteClient, remoteNode, pageNumber)
		if err != nil {
			return xfersProcessed, err
		}
		dpnSync.Logger.Debug("Got %d restore requests from %s", len(result.Results), remoteNode.Namespace)
		updated, err := dpnSync.syncRestoreRequests(result.Results)
		if err != nil {
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updated...)
		if result.Next == nil || *result.Next == "" {
			dpnSync.Logger.Debug("No more restore requests to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Logger.Debug("Processed %d restore requests in local registry", len(xfersProcessed))
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) syncRestoreRequests(xfers []*DPNRestoreTransfer) ([]*DPNRestoreTransfer, error) {
	xfersProcessed := make([]*DPNRestoreTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Logger.Debug("Processing restore transfer %s", xfer.RestoreId)
		existingXfer, _ := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
		var err error
		var updatedXfer *DPNRestoreTransfer
		if existingXfer != nil {
			if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
				dpnSync.Logger.Debug("Not updating restore request %s, because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s", xfer.RestoreId,
					xfer.UpdatedAt, existingXfer.UpdatedAt)
			} else {
				dpnSync.Logger.Debug("Restore request %s exists... updating", xfer.RestoreId)
				updatedXfer, err = dpnSync.LocalClient.RestoreTransferUpdate(xfer)
			}
		} else {
			dpnSync.Logger.Debug("Restore request %s not in local registry... creating", xfer.RestoreId)
			updatedXfer, err = dpnSync.LocalClient.RestoreTransferCreate(xfer)
		}
		if err != nil {
			dpnSync.Logger.Debug("Oops! Restore request %s: %v", xfer.RestoreId, err)
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updatedXfer)
	}
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) getRestoreRequests(remoteClient *DPNRestClient, remoteNode *DPNNode, pageNumber int) (*RestoreListResult, error) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the to_node.
	// E.g. We ask TDR for restore requests going TO TDR.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("to_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	return remoteClient.DPNRestoreListGet(&params)
}
