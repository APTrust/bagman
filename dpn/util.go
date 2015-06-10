package dpn

import (
	"crypto/sha256"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"os"
)

func GetRemoteClients(localClient *DPNRestClient, config *DPNConfig, logger *logging.Logger) (map[string]*DPNRestClient, error) {
	remoteClients := make(map[string]*DPNRestClient)
	for namespace, _ := range config.RemoteNodeTokens {
		remoteClient, err := localClient.GetRemoteClient(namespace, config, logger)
		if err != nil {
			return nil, fmt.Errorf("Error creating remote client for node %s: %v", namespace, err)
		}
		remoteClients[namespace] = remoteClient
	}
	return remoteClients, nil
}

// Run the sha256 checksum on the bag we just copied from the remote node.
func CalculateSha256Digest(filePath string) (string, error) {
	src, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer src.Close()
	shaHash := sha256.New()
	_, err = io.Copy(shaHash, src)
	if err != nil {
		detailedError := fmt.Errorf("Error calculating sha256 on %s: %v",
			filePath, err)
		return "", detailedError
	}
	return string(shaHash.Sum(nil)), nil

}
