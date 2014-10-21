// Package client provides a client for the fluctus REST API.
package bagman

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// Maximum number of generic files we can create in a single
// call to IntellectualObjectCreate. New objects with more
// than this number of files need special handling.
const MAX_FILES_FOR_CREATE = 500

// Log fluctus error responses up to this number of bytes.
// We DO want to log concise error messages. We DO NOT want
// to log huge HTML error responses.
const MAX_FLUCTUS_ERR_MSG_SIZE = 1000

// Regex to match the top-level domain suffixes we expect to see.
var domainPattern *regexp.Regexp = regexp.MustCompile("\\.edu|org|com$")

type FluctusClient struct {
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
func NewFluctusClient(hostUrl, apiVersion, apiUser, apiKey string, logger *logging.Logger) (*FluctusClient, error) {
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
	return &FluctusClient{hostUrl, apiVersion, apiUser, apiKey, httpClient, transport, logger, nil}, nil
}

// Caches a map of institutions in which institution domain name
// is the key and institution id is the value.
func (client *FluctusClient) CacheInstitutions() error {
	instUrl := client.BuildUrl("/institutions")
	client.logger.Debug("Requesting list of institutions from fluctus: %s", instUrl)
	request, err := client.NewJsonRequest("GET", instUrl, nil)
	if err != nil {
		client.logger.Error("Error building institutions request in Fluctus client:", err.Error())
		return err
	}

	body, response, err := client.doRequest(request)
	if err != nil {
		client.logger.Error("Error getting list of institutions from Fluctus", err.Error())
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("Fluctus replied to request for institutions list with status code %d",
			response.StatusCode)
	}

	// Build and return the data structure
	institutions := make([]*Institution, 1, 100)
	err = json.Unmarshal(body, &institutions)
	if err != nil {
		return client.formatJsonError("CacheInstitutions", body, err)
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
func (client *FluctusClient) BuildUrl(relativeUrl string) string {
	return client.hostUrl + relativeUrl
}

// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *FluctusClient) NewJsonRequest(method, targetUrl string, body io.Reader) (*http.Request, error) {
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
	// URLs that contain %2F (encoded slashes) MUST preserve
	// the %2F. The Go URL library silently converts those
	// to slashes, and we DON'T want that!
	// See http://stackoverflow.com/questions/20847357/golang-http-client-always-escaped-the-url/
    incorrectUrl, err := url.Parse(targetUrl)
    if err != nil {
        return nil, err
    }
    opaqueUrl := strings.Replace(targetUrl, client.hostUrl, "", 1)

    // This fixes an issue with GenericFile names that include spaces.
    opaqueUrl = strings.Replace(opaqueUrl, " ", "%20", -1)

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
func (client *FluctusClient) GetBagStatus(etag, name string, bag_date time.Time) (status *ProcessStatus, err error) {
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

// ProcessStatusSearch returns any ProcessedItem/ProcessStatus
// records from fluctus matching the specified criteria.
// Params retry and reviewed are really booleans, but there
// is no empty value for booleans in Go, so use strings
// "true", "false" or "" for no filter.
func (client *FluctusClient) ProcessStatusSearch(etag, name, stage, status, retry, reviewed string, bagDate time.Time) (statusRecords []*ProcessStatus, err error) {
	queryString := ""
	if etag != "" { queryString += fmt.Sprintf("etag=%s&", etag) }
	if name != "" { queryString += fmt.Sprintf("name=%s&", name) }
	if stage != "" { queryString += fmt.Sprintf("stage=%s&", stage) }
	if status != "" { queryString += fmt.Sprintf("status=%s&", status) }
	if retry != "" { queryString += fmt.Sprintf("retry=%s&", retry) }
	if reviewed != "" { queryString += fmt.Sprintf("reviewed=%s&", reviewed) }
	if bagDate.IsZero() == false {
		queryString += fmt.Sprintf("bag_date=%s&",
			url.QueryEscape(bagDate.Format(time.RFC3339)))
	}
	statusUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/search?%s",
		client.apiVersion, queryString))
	request, err := client.NewJsonRequest("GET", statusUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 400 or 500
	if response.StatusCode != 200 {
		message := "ProcessStatusSearch: Fluctus returned status code %d."
		err = client.buildAndLogError(body, message, response.StatusCode)
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &statusRecords)
	if err != nil {
		return nil, client.formatJsonError(statusUrl, body, err)
	}
	return statusRecords, nil
}


// GetReviewedItems returns a list of items from Fluctus's reviewed items
// from Fluctus' processed items list. It returns a list of CleanupResults.
// The cleanup task uses this list to figure out what to delete from the
// receiving buckets.
func (client *FluctusClient) GetReviewedItems() (results []*CleanupResult, err error) {
	reviewedUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/get_reviewed.json",
		client.apiVersion))

	request, err := client.NewJsonRequest("GET", reviewedUrl, nil)
	if err != nil {
		return nil, err
	}
	body, _, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	items := make([]*ProcessStatus, 0)
	err = json.Unmarshal(body, &items)
	if err != nil {
		return nil, client.formatJsonError("GetReviewedItems", body, err)
	}
	results = make([]*CleanupResult, len(items))
	for i, item := range items {
		file := &CleanupFile{
			BucketName: item.Bucket,
			Key:        item.Name,
		}
		files := make([]*CleanupFile, 1)
		files[0] = file
		cleanupResult := &CleanupResult{
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

// UpdateProcessedItem sends a message to Fluctus describing whether bag
// processing succeeded or failed. If it failed, the ProcessStatus
// object includes some details of what went wrong.
func (client *FluctusClient) UpdateProcessedItem(status *ProcessStatus) (err error) {
	relativeUrl := fmt.Sprintf("/api/%s/itemresults", client.apiVersion)
	httpMethod := "POST"
	expectedResponseCode := 201
	if status.Id > 0 {
		relativeUrl = fmt.Sprintf("/api/%s/itemresults/%d",
			client.apiVersion, status.Id)
		httpMethod = "PUT"
		expectedResponseCode = 200
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
	status, err = client.doStatusRequest(req, expectedResponseCode)
	if err != nil {
		client.logger.Error("JSON for failed Fluctus request: %s",
			string(postData))
	}
	return err
}

func (client *FluctusClient) doStatusRequest(request *http.Request, expectedStatus int) (status *ProcessStatus, err error) {
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// OK to return 404 on a status check. It just means the bag has not
	// been processed before.
	if response.StatusCode == 404 && request.Method == "GET" {
		return nil, nil
	}

	if response.StatusCode != expectedStatus {
		message := "doStatusRequest Expected status code %d but got %d. URL: %s."
		err = client.buildAndLogError(body, message, expectedStatus, response.StatusCode, request.URL)
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, client.formatJsonError(request.URL.RequestURI(), body, err)
	}
	return status, nil
}

func (client *FluctusClient) BulkStatusGet(since time.Time) (statusRecords []*ProcessStatus, err error) {
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/ingested_since/%s",
		client.apiVersion, url.QueryEscape(since.UTC().Format(time.RFC3339))))
	client.logger.Debug("Requesting bulk bag status from fluctus: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 400 or 500
	if response.StatusCode != 200 {
		message := "Request for bulk status returned status code %d."
		err = client.buildAndLogError(body, message, response.StatusCode)
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &statusRecords)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return statusRecords, nil
}


/*
Returns a list of items that need to be restored.
If param objectIdentifier is not an empty string, this
will return all ProcessedItem records for the intellectual
object that are in action "Restore".

If no objectIdentifier is supplied, this returns all ProcessedItem
records in action "Restore" with stage "Requested" and status
"Pending".

This will return zero items in either of the following cases:

1. No objectIdentifier is supplied and there are no pending
restoration requests in Fluctus' ProcessedItems table.

2. An objectIdentifier is supplied, and there are no
ProcessedItem records for that object in stage Restore.
*/
func (client *FluctusClient) RestorationItemsGet(objectIdentifier string) (statusRecords []*ProcessStatus, err error) {
	return client.getStatusItemsForQueue("restore", objectIdentifier)
}


/*
Returns a list of items that need to be deleted.
If param genericFileIdentifier is not an empty string, this
will return all ProcessedItem records for the generic file
that have Action == "Delete".

If no genericFileIdentifier is supplied, this returns all ProcessedItem
records in action "Delete" with stage "Requested" and status
"Pending".

This will return zero items in either of the following cases:

1. No genericFileIdentifier is supplied and there are no pending
restoration requests in Fluctus' ProcessedItems table.

2. A genericFileIdentifier is supplied, and there are no
ProcessedItem records for that object in stage Restore.
*/
func (client *FluctusClient) DeletionItemsGet(genericFileIdentifier string) (statusRecords []*ProcessStatus, err error) {
	return client.getStatusItemsForQueue("delete", genericFileIdentifier)
}

// Calls one of the ProcessedItem endpoints that returns a list of ProcessedItems.
func (client *FluctusClient) getStatusItemsForQueue(itemType, identifier string) (statusRecords []*ProcessStatus, err error) {
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/items_for_restore.json", client.apiVersion))
	paramName := "object_identifier"
	if itemType == "delete" {
		objUrl = client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/items_for_delete.json", client.apiVersion))
		paramName = "generic_file_identifier"
	}
	if identifier != "" {
		objUrl = fmt.Sprintf("%s?%s=%s", objUrl, paramName, identifier)
	}
	client.logger.Debug("Getting list of %s items from fluctus: %s", itemType, objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// Check for error response
	if response.StatusCode != 200 {
		message := "Request for %s records returned status code %d."
		err = client.buildAndLogError(body, message, itemType, response.StatusCode)
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &statusRecords)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return statusRecords, nil
}



// Returns the IntellectualObject with the specified id, or nil of no
// such object exists. If includeRelations is false, this returns only
// the IntellectualObject. If includeRelations is true, this returns
// the IntellectualObject with all of its GenericFiles and Events.
// Param identifier must have slashes replaced with %2F or you'll get a 404!
func (client *FluctusClient) IntellectualObjectGet(identifier string, includeRelations bool) (*IntellectualObject, error) {
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
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Build and return the data structure
	obj := &IntellectualObject{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

// Updates an existing IntellectualObject in fluctus.
// Returns the IntellectualObject.
func (client *FluctusClient) IntellectualObjectUpdate(obj *IntellectualObject) (newObj *IntellectualObject, err error) {
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
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// Fluctus returns 204 (No content) on update
	if response.StatusCode != 204 {
		message := "IntellectualObjectSave Expected status code 204 but got %d. URL: %s."
		err = client.buildAndLogError(body, message, response.StatusCode, request.URL)
		return nil, err
	} else {
		client.logger.Debug("%s IntellectualObject %s succeeded", method, obj.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newObj = &IntellectualObject{}
		err = json.Unmarshal(body, newObj)
		if err != nil {
			return nil, client.formatJsonError(objUrl, body, err)
		}
		return newObj, nil
	} else {
		return obj, nil
	}
}

func (client *FluctusClient) IntellectualObjectCreate(obj *IntellectualObject, maxGenericFiles int) (newObj *IntellectualObject, err error) {
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

	data, err := obj.SerializeForCreate(maxGenericFiles)
	request, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 201 {
		message := "IntellectualObjectCreate Expected status code 201 but got %d. URL: %s"
		err = client.buildAndLogError(body, message, response.StatusCode, request.URL)
		return nil, err
	} else {
		client.logger.Debug("%s IntellectualObject %s succeeded", method, obj.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newObj = &IntellectualObject{}
		err = json.Unmarshal(body, newObj)
		if err != nil {
			return nil, client.formatJsonError(objUrl, body, err)
		}
		return newObj, nil
	} else {
		return obj, nil
	}
}

// Returns the generic file with the specified identifier.
func (client *FluctusClient) GenericFileGet(genericFileIdentifier string, includeRelations bool) (*GenericFile, error) {
	queryString := ""
	if includeRelations == true {
		queryString = "include_relations=true"
	}
	fileUrl := client.BuildUrl(fmt.Sprintf("/api/%s/files/%s?%s",
		client.apiVersion,
		escapeSlashes(genericFileIdentifier),
		queryString))
	request, err := client.NewJsonRequest("GET", fileUrl, nil)
	if err != nil {
		return nil, err
	}
	client.logger.Debug("Requesting GenericFile from fluctus: %s", request.URL.RequestURI())
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Build and return the data structure
	obj := &GenericFile{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(fileUrl, body, err)
	}
	return obj, nil
}

// Saves a GenericFile to fluctus. This function
// figures out whether the save is a create or an update.
// Param objId is the Id of the IntellectualObject to which
// the file belongs. This returns the GenericFile.
func (client *FluctusClient) GenericFileSave(objId string, gf *GenericFile) (newGf *GenericFile, err error) {
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
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 && response.StatusCode != 204 {
		err = fmt.Errorf("GenericFileSave Expected status code 201 or 204 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		//if len(body) < 1000 {
		client.logger.Error(err.Error(), strings.Replace(string(body), "\n", " ", -1))
		//} else {
		//	client.logger.Error(err.Error())
		//}
		return nil, err
	} else {
		client.logger.Debug("%s GenericFile %s succeeded", method, gf.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		newGf = &GenericFile{}
		err = json.Unmarshal(body, newGf)
		if err != nil {
			return nil, client.formatJsonError(request.URL.RequestURI(), body, err)
		}
		return newGf, nil
	} else {
		return gf, nil
	}
}

// Saves a batch of GenericFiles to fluctus. This is
// for create only.
func (client *FluctusClient) GenericFileSaveBatch(objId string, files []*GenericFile) (err error) {
	// URL & method for create
	fileUrl := client.BuildUrl(fmt.Sprintf("/api/%s/objects/%s/files/save_batch",
		client.apiVersion, escapeSlashes(objId)))
	method := "POST"

	client.logger.Debug("About to POST %d GenericFiles to Fluctus for object %s",
		len(files), objId)

	// HACK! WTF??
	// Why does Fluctus sometimes want 'checksums'
	// and sometimes 'checksum_attributes'????
	postData := make(map[string][]map[string]interface{})
	postData["generic_files"] = GenericFilesToMaps(files)
	for i := range postData["generic_files"] {
		gf := postData["generic_files"][i]
		gf["checksum_attributes"] = gf["checksum"]
		delete(gf, "checksum")
	}

	data, err := json.Marshal(postData)
	if err != nil {
		return fmt.Errorf("GenericFileSaveBatch() cannot convert files to json: %v", err)
	}

	request, err := client.NewJsonRequest(method, fileUrl, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return err
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 {
		fmt.Println(string(body))
		err = fmt.Errorf("GenericFileSaveBatch Expected status code 201 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		client.logger.Error(err.Error(), strings.Replace(string(body), "\n", " ", -1))
		return err
	} else {
		client.logger.Debug("Post GenericFileBatch succeeded for %d files", len(files))
	}
	return nil
}


// Saves a PremisEvent to Fedora. Param objId should be the IntellectualObject id
// if you're recording an object-related event, such as ingest; or a GenericFile id
// if you're recording a file-related event, such as fixity generation.
// Param objType must be either "IntellectualObject" or "GenericFile".
// Param event is the event you wish to save. This returns the event that comes
// back from Fluctus. Note that you can create events, but you cannot update them.
// All saves will create new events!
func (client *FluctusClient) PremisEventSave(objId, objType string, event *PremisEvent) (newEvent *PremisEvent, err error) {
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
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 201 {
		message := "PremisEventSave Expected status code 201 but got %d. URL: %s."
		err = client.buildAndLogError(body, message, response.StatusCode, request.URL)
		return nil, err
	} else {
		client.logger.Debug("%s PremisEvent %s for objId %s succeeded", method, event.EventType, objId)
	}

	// Fluctus should always return the newly created event
	newEvent = &PremisEvent{}
	err = json.Unmarshal(body, newEvent)
	if err != nil {
		return nil, client.formatJsonError(request.URL.RequestURI(), body, err)
	}
	return newEvent, nil
}

// Replaces "/" with "%2F", which golang's url.QueryEscape does not do.
func escapeSlashes(s string) string {
	return strings.Replace(s, "/", "%2F", -1)
}

// SendProcessedItem sends information about the status of
// processing this item to Fluctus. Param localStatus should come from
// ProcessResult.ProcessStatus(), which gives information about
// the current state of processing.
func (client *FluctusClient) SendProcessedItem(localStatus *ProcessStatus) (err error) {
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
	err = client.UpdateProcessedItem(localStatus)
	if err != nil {
		return err
	}
	client.logger.Info("Updated status in Fluctus for %s: %s/%s",
		localStatus.Name, localStatus.Stage, localStatus.Status)
	return nil
}

/*
This sets the status of the bag restore operation on all
ProcessedItem records for all bag parts that make up the
current object. If an object was uploaded as a series of
100 bags, this sets the status on the processed item records
for the latest ingested version of each of those 100 bags.
*/
func (client *FluctusClient) RestorationStatusSet(objectIdentifier string, stage StageType, status StatusType, note string, retry bool) (error) {
	if objectIdentifier == "" {
		return fmt.Errorf("Object identifier cannot be empty.")
	}
	objUrl := client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/restoration_status/%s",
		client.apiVersion, escapeSlashes(objectIdentifier)))
	client.logger.Debug("Setting restoration status: %s - stage = %s, status = %s, retry = %s",
		objUrl, stage, status, retry)
	data := make(map[string]interface{})
	data["stage"] = stage
	data["status"] = status
	data["retry"] = retry
	data["note"] = note
	jsonData, err := json.Marshal(data)
	request, err := client.NewJsonRequest("POST", objUrl, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("Could not build POST request for %s: %v", objUrl, err)
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return fmt.Errorf("Error executing POST request for %s: %v", objUrl, err)
	}

	// Check for error response
	if response.StatusCode != 200 {
		message := "RestorationStatusSet returned status code %d."
		err = client.buildAndLogError(body, message, response.StatusCode)
		return err
	}

	return nil
}

// Delete the data we created with our integration tests
func (client *FluctusClient) DeleteFluctusTestData() error {
	urls := make([]string, 1)
	urls[0] = client.BuildUrl(fmt.Sprintf("/api/%s/itemresults/delete_test_items.json", client.apiVersion))
	for _, url := range urls {
		request, err := client.NewJsonRequest("POST", url, nil)
		if err != nil {
			client.logger.Error("Error building request for %s: %v", url, err.Error())
			return err
		}
		response, err := client.httpClient.Do(request)
		if err != nil {
			client.logger.Error("Error posting to %s: %v", url, err.Error())
			return err
		}

		if response.StatusCode != 200 {
			return fmt.Errorf("Fluctus replied to POST %s with status code %d",
				url, response.StatusCode)
		}

		_, err = readResponse(response.Body)
		if err != nil {
			return err
		}

	}
	return nil
}

// Reads the response body and returns a byte slice.
// You must read and close the response body, or the
// TCP connection will remain open for as long as
// our application runs.
func readResponse(body io.ReadCloser) (data []byte, err error) {
	if body != nil {
		data, err = ioutil.ReadAll(body)
		body.Close()
	}
	return data, err
}

func (client *FluctusClient) doRequest(request *http.Request) (data []byte, response *http.Response, err error) {
	response, err = client.httpClient.Do(request)
	if err != nil {
		return nil, nil, err
	}
	data, err = readResponse(response.Body)
	if err != nil {
		return nil, response, err
	}
	return data, response, err
}

func (client *FluctusClient) buildAndLogError(body []byte, formatString string, args ...interface{}) (err error) {
	if len(body) < MAX_FLUCTUS_ERR_MSG_SIZE {
		formatString += " Response body: %s"
		args = append(args, string(body))
	}
	err = fmt.Errorf(formatString, args)
	client.logger.Error(err.Error())
	return err
}

func (client *FluctusClient) formatJsonError(callerName string, body []byte, err error) (error) {
	json := strings.Replace(string(body), "\n", " ", -1)
	return fmt.Errorf("%s: Error parsing JSON response: %v -- JSON response: %s", err, json)
}
