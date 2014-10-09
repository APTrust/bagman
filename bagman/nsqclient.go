package bagman

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// Sends the JSON of a result object to the specified queue.
func Enqueue(nsqdHttpAddress, topic string, result *ProcessResult) error {
	key := result.S3File.Key.Key
	url := fmt.Sprintf("%s/put?topic=%s", nsqdHttpAddress, topic)
	json, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("Error marshalling result for '%s' to JSON for file: %v", key, err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(json))

	if err != nil {
		return fmt.Errorf("Nsqd returned an error when queuing '%s': %v", key, err)
	}
	if resp == nil {
		return fmt.Errorf("No response from nsqd at '%s'. Is it running?", url)
	}

	// nsqd sends a simple OK. We have to read the response body,
	// or the connection will hang open forever.
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyText := "[no response body]"
		if len(body) > 0 {
			bodyText = string(body)
		}
		return fmt.Errorf("nsqd returned status code %d when attempting to queue %s. " +
			"Response body: %s",
			resp.StatusCode, key, bodyText)
	}
	return nil
}
