// Package client provides a client for the fluctus REST API.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var domainPattern *regexp.Regexp = regexp.MustCompile("\\.edu|org|com$")

type Client struct {
	hostUrl      string
	apiVersion   string
	apiUser      string
	apiKey       string
	httpClient   *http.Client
	transport    *http.Transport
	logger       *logging.Logger
	institutions map[string]string
}

// Creates a new fluctus client. Param hostUrl should come from
// the config.json file.
func New(hostUrl, apiVersion, apiUser, apiKey string, logger *logging.Logger) (*Client, error) {
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		DisableKeepAlives:   false,
	}
	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	return &Client{hostUrl, apiVersion, apiUser, apiKey, httpClient, transport, logger, nil}, nil
}

// Caches a map of institutions in which institution domain name
// is the key and institution id is the value.
func (client *Client) CacheInstitutions() error {
	instUrl := client.BuildUrl("/institutions")
	client.logger.Debug("Requesting list of institutions from fluctus: %s", instUrl)
	request, err := client.NewJsonRequest("GET", instUrl, nil)
	if err != nil {
		client.logger.Error("Error building institutions request in Fluctus client:", err.Error())
		return err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		client.logger.Error("Error getting list of institutions from Fluctus", err.Error())
		return err
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("Fluctus replied to request for institutions list with status code %d",
			response.StatusCode)
	}

	// Read the json response
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	// Build and return the data structure
	institutions := make([]*models.Institution, 1, 100)
	err = json.Unmarshal(body, &institutions)
	if err != nil {
		return err
	}

	client.institutions = make(map[string]string, len(institutions))
	for _, inst := range institutions {
		client.institutions[inst.Identifier] = inst.Pid
	}
	return nil

}

// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *Client) BuildUrl(relativeUrl string) string {
	return client.hostUrl + relativeUrl
}

// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *Client) NewJsonRequest(method, targetUrl string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, targetUrl, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Fluctus-API-User", client.apiUser)
	req.Header.Add("X-Fluctus-API-Key", client.apiKey)
	req.Header.Add("Connection", "Keep-Alive")

	// Unfix the URL that golang net/url "fixes" for us.
	// See http://stackoverflow.com/questions/20847357/golang-http-client-always-escaped-the-url/
	incorrectUrl, err := url.Parse(targetUrl)
	if err != nil {
		return nil, err
	}
	opaqueUrl := strings.Replace(targetUrl, client.hostUrl, "", 1)
	correctUrl := &url.URL{
		Scheme: incorrectUrl.Scheme,
		Host:   incorrectUrl.Host,
		Opaque: opaqueUrl,
	}
	req.URL = correctUrl
	return req, nil
}

// GetBagStatus returns the status of a bag from a prior round of processing.
// This function will return nil if Fluctus has no record of this bag.
func (client *Client) GetBagStatus(etag, name string, bag_date time.Time) (status *bagman.ProcessStatus, err error) {
	statusUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/%s/%s/%s",
		client.apiVersion, etag, name,
		url.QueryEscape(bag_date.Format(time.RFC3339))))
	req, err := client.NewJsonRequest("GET", statusUrl, nil)
	if err != nil {
		return nil, err
	}
	status, err = client.doStatusRequest(req, 200)
	return status, err
}

// GetReviewedItems returns a list of items from Fluctus's reviewed items
// from Fluctus' processed items list. It returns a list of CleanupResults.
// The cleanup task uses this list to figure out what to delete from the
// receiving buckets.
func (client *Client) GetReviewedItems() (results []*bagman.CleanupResult, err error) {
	reviewedUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/get_reviewed.json",
		client.apiVersion))
	request, err := client.NewJsonRequest("GET", reviewedUrl, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	items := make([]*bagman.ProcessStatus, 0)
	err = json.Unmarshal(body, &items)
	if err != nil {
		return nil, err
	}
	results = make([]*bagman.CleanupResult, len(items))
	for i, item := range items {
		file := &bagman.CleanupFile{
			BucketName: item.Bucket,
			Key:        item.Name,
		}
		files := make([]*bagman.CleanupFile, 1)
		files[0] = file
		cleanupResult := &bagman.CleanupResult{
			BagName:          item.Name,
			ETag:             item.ETag,
			BagDate:          item.BagDate,
			ObjectIdentifier: "",
			Files:            files,
		}
		results[i] = cleanupResult
	}
	return results, nil
}

// UpdateBagStatus sends a message to Fluctus describing whether bag
// processing succeeded or failed. If it failed, the ProcessStatus
// object includes some details of what went wrong.
func (client *Client) UpdateBagStatus(status *bagman.ProcessStatus) (err error) {
	relativeUrl := fmt.Sprintf("/api/%s/itemresults", client.apiVersion)
	httpMethod := "POST"
	if status.Id > 0 {
		relativeUrl = fmt.Sprintf("/api/%s/itemresults/%s/%s/%s",
			client.apiVersion, status.ETag, status.Name,
			status.BagDate.Format(time.RFC3339))
		httpMethod = "PUT"
	}
	statusUrl := client.BuildUrl(relativeUrl)
	postData, err := status.SerializeForFluctus()
	if err != nil {
		return err
	}
	req, err := client.NewJsonRequest(httpMethod, statusUrl, bytes.NewBuffer(postData))
	if err != nil {
		return err
	}
	status, err = client.doStatusRequest(req, 201)
	if err != nil {
		client.logger.Error("JSON for failed Fluctus request: %s",
			string(postData))
	}
	return err
}

func (client *Client) doStatusRequest(request *http.Request, expectedStatus int) (status *bagman.ProcessStatus, err error) {
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// We have to read the response body, whether we're interested in
	// its contents or not. If we don't read to the end of it, we'll
	// end up with thousands of TCP connections in CLOSED_WAIT state,
	// and the system will run out of file handles. See this post:
	// http://stackoverflow.com/questions/17948827/reusing-http-connections-in-golang
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// OK to return 404 on a status check. It just means the bag has not
	// been processed before.
	if response.StatusCode == 404 && request.Method == "GET" {
		return nil, nil
	}

	if response.StatusCode != expectedStatus {
		if len(body) < 1000 {
			err = fmt.Errorf("doStatusRequest Expected status code %d but got %d. "+
				"URL: %s, Response: %s",
				expectedStatus, response.StatusCode, request.URL, string(body))
		} else {
			err = fmt.Errorf("doStatusRequest Expected status code %d but got %d. URL: %s",
				expectedStatus, response.StatusCode, request.URL)
		}
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, err
	}
	return status, nil
}

func (client *Client) BulkStatusGet(since time.Time) (statusRecords []*bagman.ProcessStatus, err error) {
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/ingested_since/%s",
		client.apiVersion, url.QueryEscape(since.UTC().Format(time.RFC3339))))
	client.logger.Debug("Requesting bulk bag status from fluctus: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// 400 or 500
	if response.StatusCode != 200 {
		if len(body) < 1000 {
			return nil, fmt.Errorf("Request for bulk status returned status code %d. "+
				"Response body: %s", response.StatusCode, string(body))
		} else {
			return nil, fmt.Errorf("Request returned status code %d", response.StatusCode)
		}
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &statusRecords)
	if err != nil {
		return nil, err
	}
	return statusRecords, nil
}

// Returns the IntellectualObject with the specified id, or nil of no
// such object exists. If includeRelations is false, this returns only
// the IntellectualObject. If includeRelations is true, this returns
// the IntellectualObject with all of its GenericFiles and Events.
// Param identifier must have slashes replaced with %2F or you'll get a 404!
func (client *Client) IntellectualObjectGet(identifier string, includeRelations bool) (*models.IntellectualObject, error) {
	queryString := ""
	if includeRelations == true {
		queryString = "include_relations=true"
	}
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/objects/%s?%s",
		client.apiVersion, escapeSlashes(identifier), queryString))
	client.logger.Debug("Requesting IntellectualObject from fluctus: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Build and return the data structure
	obj := &models.IntellectualObject{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// Updates an existing IntellectualObject in fluctus.
// Returns the IntellectualObject.
func (client *Client) IntellectualObjectUpdate(obj *models.IntellectualObject) (newObj *models.IntellectualObject, err error) {
	if obj == nil {
		return nil, fmt.Errorf("Param obj cannot be nil")
	}

	if client.institutions == nil || len(client.institutions) == 0 {
		err = client.CacheInstitutions()
		if err != nil {
			client.logger.Error("Fluctus client can't build institutions cache: %v", err)
			return nil, fmt.Errorf("Error building institutions cache: %v", err)
		}
	}

	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/objects/%s",
		client.apiVersion, escapeSlashes(obj.Identifier)))
	method := "PUT"

	client.logger.Debug("About to %s IntellectualObject %s to Fluctus", method, obj.Identifier)

	data, err := obj.SerializeForFluctus()
	request, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// Fluctus returns 204 (No content) on update
	if response.StatusCode != 204 {
		if len(body) < 1000 {
			err = fmt.Errorf("IntellectualObjectSave Expected status code 204 but got %d. "+
				"URL: %s, Response body: %s\n",
				response.StatusCode, request.URL, string(body))
		} else {
			err = fmt.Errorf("IntellectualObjectSave Expected status code 204 but got %d. URL: %s\n",
				response.StatusCode, request.URL)
		}
		client.logger.Error(err.Error())
		return nil, err
	} else {
		client.logger.Debug("%s IntellectualObject %s succeeded", method, obj.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newObj = &models.IntellectualObject{}
		err = json.Unmarshal(body, newObj)
		if err != nil {
			return nil, err
		}
		return newObj, nil
	} else {
		return obj, nil
	}
}

func (client *Client) IntellectualObjectCreate(obj *models.IntellectualObject) (newObj *models.IntellectualObject, err error) {
	if obj == nil {
		return nil, fmt.Errorf("Param obj cannot be nil")
	}

	if client.institutions == nil || len(client.institutions) == 0 {
		err = client.CacheInstitutions()
		if err != nil {
			client.logger.Error("Fluctus client can't build institutions cache: %v", err)
			return nil, fmt.Errorf("Error building institutions cache: %v", err)
		}
	}

	// URL & method for create
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/objects/include_nested.json?include_nested=true",
		client.apiVersion))
	method := "POST"

	client.logger.Debug("About to %s IntellectualObject %s to Fluctus", method, obj.Identifier)

	data, err := obj.SerializeForCreate()
	request, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	if response.StatusCode != 201 {
		if len(body) < 1000 {
			err = fmt.Errorf("IntellectualObjectCreate Expected status code 201 but got %d. "+
				"URL: %s, Response body: %s\n",
				response.StatusCode, request.URL, string(body))
		} else {
			err = fmt.Errorf("IntellectualObjectCreate Expected status code 201 but got %d. URL: %s\n",
				response.StatusCode, request.URL)
		}
		client.logger.Error(err.Error())
		return nil, err
	} else {
		client.logger.Debug("%s IntellectualObject %s succeeded", method, obj.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newObj = &models.IntellectualObject{}
		err = json.Unmarshal(body, newObj)
		if err != nil {
			return nil, err
		}
		return newObj, nil
	} else {
		return obj, nil
	}
}

// Returns the generic file with the specified identifier.
func (client *Client) GenericFileGet(genericFileIdentifier string, includeRelations bool) (*models.GenericFile, error) {
	queryString := ""
	if includeRelations == true {
		queryString = "include_relations=true"
	}
	fileUrl := client.BuildUrl(fmt.Sprintf("/api/%s/files/%s?%s",
		client.apiVersion,
		escapeSlashes(genericFileIdentifier),
		queryString))
	client.logger.Debug("Requesting IntellectualObject from fluctus: %s", fileUrl)
	request, err := client.NewJsonRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Build and return the data structure
	obj := &models.GenericFile{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// Saves an GenericFile to fluctus. This function
// figures out whether the save is a create or an update.
// Param objId is the Id of the IntellectualObject to which
// the file belongs. This returns the GenericFile.
func (client *Client) GenericFileSave(objId string, gf *models.GenericFile) (newGf *models.GenericFile, err error) {
	existingObj, err := client.GenericFileGet(gf.Identifier, false)
	if err != nil {
		return nil, err
	}
	// URL & method for create
	fileUrl := client.BuildUrl(fmt.Sprintf("/api/%s/objects/%s/files.json",
		client.apiVersion, escapeSlashes(objId)))
	method := "POST"
	// URL & method for update
	if existingObj != nil {
		fileUrl = client.BuildUrl(fmt.Sprintf("/api/%s/files/%s",
			client.apiVersion, escapeSlashes(gf.Identifier)))
		method = "PUT"
	}

	client.logger.Debug("About to %s GenericFile %s to Fluctus", method, gf.Identifier)

	data, err := gf.SerializeForFluctus()
	request, err := client.NewJsonRequest(method, fileUrl, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 && response.StatusCode != 204 {
		err = fmt.Errorf("GenericFileSave Expected status code 201 or 204 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		if len(body) < 1000 {
			client.logger.Error(err.Error(), string(body))
		} else {
			client.logger.Error(err.Error())
		}
		return nil, err
	} else {
		client.logger.Debug("%s GenericFile %s succeeded", method, gf.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newGf = &models.GenericFile{}
		err = json.Unmarshal(body, newGf)
		if err != nil {
			return nil, err
		}
		return newGf, nil
	} else {
		return gf, nil
	}
}

// Saves a PremisEvent to Fedora. Param objId should be the IntellectualObject id
// if you're recording an object-related event, such as ingest; or a GenericFile id
// if you're recording a file-related event, such as fixity generation.
// Param objType must be either "IntellectualObject" or "GenericFile".
// Param event is the event you wish to save. This returns the event that comes
// back from Fluctus. Note that you can create events, but you cannot update them.
// All saves will create new events!
func (client *Client) PremisEventSave(objId, objType string, event *models.PremisEvent) (newEvent *models.PremisEvent, err error) {
	if objId == "" {
		return nil, fmt.Errorf("Param objId cannot be empty")
	}
	if objType != "IntellectualObject" && objType != "GenericFile" {
		return nil, fmt.Errorf("Param objType must be either 'IntellectualObject' or 'GenericFile'")
	}
	if event == nil {
		return nil, fmt.Errorf("Param event cannot be nil")
	}

	method := "POST"
	eventUrl := client.BuildUrl(fmt.Sprintf("/api/%s/files/%s/events",
		client.apiVersion, escapeSlashes(objId)))
	if objType == "IntellectualObject" {
		eventUrl = client.BuildUrl(fmt.Sprintf("/api/%s/objects/%s/events",
			client.apiVersion, escapeSlashes(objId)))
	}

	client.logger.Debug("Creating %s PremisEvent %s for objId %s", objType, event.EventType, objId)

	data, err := json.Marshal(event)
	request, err := client.NewJsonRequest(method, eventUrl, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// Must read body. See comment above.
	var body []byte
	if response.Body != nil {
		body, err = ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	if response.StatusCode != 201 {
		if len(body) < 1000 {
			err = fmt.Errorf("PremisEventSave Expected status code 201 but got %d. "+
				"URL: %s, Response Body: %s\n",
				response.StatusCode, request.URL, string(body))
		} else {
			err = fmt.Errorf("PremisEventSave Expected status code 201 but got %d. URL: %s\n",
				response.StatusCode, request.URL)
		}
		client.logger.Error(err.Error())
		return nil, err
	} else {
		client.logger.Debug("%s PremisEvent %s for objId %s succeeded", method, event.EventType, objId)
	}

	// Fluctus should always return the newly created event
	newEvent = &models.PremisEvent{}
	err = json.Unmarshal(body, newEvent)
	if err != nil {
		return nil, err
	}
	return newEvent, nil
}

// Replaces "/" with "%2F", which golang's url.QueryEscape does not do.
func escapeSlashes(s string) string {
	return strings.Replace(s, "/", "%2F", -1)
}

// SendProcessedItem sends information about the status of
// processing this item to Fluctus. Param localStatus should come from
// bagman.ProcessResult.ProcessStatus(), which gives information about
// the current state of processing.
func (client *Client) SendProcessedItem(localStatus *bagman.ProcessStatus) (err error) {
	// Look up the status record in Fluctus. It should already exist.
	// We want to get its ID and update the existing record, rather
	// than creating a new record. Each bag should have no more than
	// one ProcessedItem record.
	remoteStatus, err := client.GetBagStatus(
		localStatus.ETag, localStatus.Name, localStatus.BagDate)
	if err != nil {
		return err
	}
	if remoteStatus != nil {
		localStatus.Id = remoteStatus.Id
	}
	err = client.UpdateBagStatus(localStatus)
	if err != nil {
		return err
	}
	client.logger.Info("Updated status in Fluctus for %s: %s/%s",
		localStatus.Name, localStatus.Stage, localStatus.Status)
	return nil
}
