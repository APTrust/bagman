package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"net/url"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// cleanup.go cleans up files in the DPN staging area.
// This typically runs as a cron job.

type Cleanup struct {
	DPNConfig           *DPNConfig
	ProcUtil            *bagman.ProcessUtil
	LocalClient         *DPNRestClient
}

func NewCleanup(procUtil *bagman.ProcessUtil, dpnConfig *DPNConfig) (*Cleanup, error) {
	localClient, err := NewDPNRestClient(
		dpnConfig.RestClient.LocalServiceURL,
		dpnConfig.RestClient.LocalAPIRoot,
		dpnConfig.RestClient.LocalAuthToken,
		dpnConfig.LocalNode,
		dpnConfig,
		procUtil.MessageLog)
	if err != nil {
		return nil, err
	}
	return &Cleanup {
		DPNConfig: dpnConfig,
		ProcUtil: procUtil,
		LocalClient: localClient,
	}, nil
}

func (cleanup *Cleanup) DeleteReplicatedBags() {
	cleanup.ProcUtil.MessageLog.Info("Deleting replicated bags in %s",
		cleanup.ProcUtil.Config.DPNStagingDirectory)
	files, err := ioutil.ReadDir(cleanup.ProcUtil.Config.DPNStagingDirectory)
	if err != nil {
		cleanup.ProcUtil.MessageLog.Error(err.Error())
		return
	}
	for _, finfo := range files {
		bagUUID := strings.Replace(finfo.Name(), ".tar", "", 1)
		if !bagman.LooksLikeUUID(bagUUID) {
			continue  // Don't delete it if it's not a DPN tar file
		}
		params := &url.Values{}
		params.Set("uuid", bagUUID)
		params.Set("status", "stored") // stored == 4
		params.Set("from_node", cleanup.DPNConfig.LocalNode)
		result, err := cleanup.LocalClient.DPNReplicationListGet(params)
		if err != nil {
			cleanup.ProcUtil.MessageLog.Error("Error getting replication info for bag '%s': %v",
				bagUUID, err.Error())
			continue
		}
		tarfile := filepath.Join(cleanup.ProcUtil.Config.DPNStagingDirectory, finfo.Name())
		if result.Count >= int32(cleanup.DPNConfig.ReplicateToNumNodes) {
			cleanup.ProcUtil.MessageLog.Info("Deleting %s: %d successful replications",
				tarfile, result.Count)
			err = os.Remove(tarfile)
			for _, xfer := range result.Results {
				symlink := fmt.Sprintf("%s/dpn.%s/outbound/%s.tar",
					cleanup.ProcUtil.Config.DPNHomeDirectory, xfer.ToNode, bagUUID)
				cleanup.ProcUtil.MessageLog.Info("Deleting symlink at %s", symlink)
			}
		} else {
			cleanup.ProcUtil.MessageLog.Info("Leaving %s: %d successful replications",
				tarfile, result.Count)
		}
	}
}
