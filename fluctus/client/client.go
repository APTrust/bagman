// Package client provides a client for the fluctus REST API.
package client

import (
	"net/http"
	"net/url"
	"net/http/cookiejar"
	"io"
	"io/ioutil"
	"encoding/json"
	"regexp"
	"fmt"
	"bytes"
	"log"
	"time"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
)

var domainPattern *regexp.Regexp = regexp.MustCompile("\\.edu|org|com$")

type Client struct {
	hostUrl        string
	apiUser        string
	apiKey         string
	httpClient     *http.Client
	transport      *http.Transport
	logger         *log.Logger
	institutions   map[string]string
}

// Creates a new fluctus client. Param hostUrl should come from
// the config.json file.
func New(hostUrl, apiUser, apiKey string, logger *log.Logger) (*Client, error) {
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}
	transport := &http.Transport{ MaxIdleConnsPerHost: 12 }
	httpClient := &http.Client{ Jar: cookieJar, Transport: transport }
	return &Client{hostUrl, apiUser, apiKey, httpClient, transport, logger, nil}, nil
}

// Caches a map of institutions in which institution domain name
// is the key and institution id is the value.
func (client *Client) CacheInstitutions () (error) {
	url := client.BuildUrl("/institutions")
	client.logger.Println("[INFO] Requesting list of institutions from fluctus:", url)
	request, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		client.logger.Println("[ERROR] Error building institutions request in Fluctus client:", err.Error())
		return err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		client.logger.Println("[ERROR] Error getting list of institutions from Fluctus", err.Error())
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
	for _, inst := range(institutions) {
		client.institutions[inst.Identifier] = inst.Pid
	}
	return nil

}

// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *Client) BuildUrl (relativeUrl string) (absoluteUrl *url.URL) {
	absoluteUrl, err := url.Parse(client.hostUrl + relativeUrl)
	if err != nil {
		// TODO: Check validity of Fluctus url on app init,
		// so we don't have to panic here.
		panic(fmt.Sprintf("Can't parse URL '%s': %v", absoluteUrl, err))
	}
	return absoluteUrl
}


// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *Client) NewJsonRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Fluctus-API-User", client.apiUser)
	req.Header.Add("X-Fluctus-API-Key", client.apiKey)
	return req, nil
}


// GetBagStatus returns the status of a bag from a prior round of processing.
// This function will return nil if Fluctus has no record of this bag.
func (client *Client) GetBagStatus(etag, name string, bag_date time.Time) (status *bagman.ProcessStatus, err error) {
	url := client.BuildUrl(fmt.Sprintf("/itemresults/%s/%s/%s", etag, name, bag_date.Format(time.RFC3339)))
	req, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	status, err = client.doStatusRequest(req, 200)
	return status, err
}

// UpdateBagStatus sends a message to Fluctus describing whether bag
// processing succeeded or failed. If it failed, the ProcessStatus
// object includes some details of what went wrong.
func (client *Client) UpdateBagStatus(status *bagman.ProcessStatus) (err error) {
	relativeUrl := "/itemresults"
	httpMethod := "POST"
	if status.Id > 0 {
		relativeUrl = fmt.Sprintf("/itemresults/%s/%s/%s",
			status.ETag, status.Name, status.BagDate.Format(time.RFC3339))
		httpMethod = "PUT"
	}
	url := client.BuildUrl(relativeUrl)
	postData, err := status.SerializeForFluctus()
	if err != nil {
		return err
	}
	req, err := client.NewJsonRequest(httpMethod, url.String(), bytes.NewBuffer(postData))
	if err != nil {
		return err
	}
	status, err = client.doStatusRequest(req, 201)
	if err != nil {
		client.logger.Printf("[ERROR] JSON for failed Fluctus request: %s",
			string(postData))
	}
	return err
}

func (client *Client) doStatusRequest(request *http.Request, expectedStatus int) (status *bagman.ProcessStatus, err error) {
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// OK to return 404 on a status check. It just means the bag has not
	// been processed before.
	if response.StatusCode == 404 && request.Method == "GET" {
		return nil, nil
	}

	if response.StatusCode != expectedStatus {
		err = fmt.Errorf("Expected status code %d but got %d. URL: %s",
			expectedStatus, response.StatusCode, request.URL)
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, err
	}
	return status, nil
}

// Returns the IntellectualObject with the specified id, or nil of no
// such object exists. If includeRelations is false, this returns only
// the IntellectualObject. If includeRelations is true, this returns
// the IntellectualObject with all of its GenericFiles and Events.
func (client *Client) IntellectualObjectGet (identifier string, includeRelations bool) (*models.IntellectualObject, error) {
	queryString := ""
	if includeRelations == true {
		queryString = "include_relations=true"
	}
	url := client.BuildUrl(fmt.Sprintf("/objects/%s?%s", identifier, queryString))
	client.logger.Println("[INFO] Requesting IntellectualObject from fluctus:", url)
	request, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Read the json response
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Build and return the data structure
	obj := &models.IntellectualObject{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}


// Saves an IntellectualObject to fluctus. This function
// figures out whether the save is a create or an update.
// It returns the IntellectualObject.
func (client *Client) IntellectualObjectSave (obj *models.IntellectualObject) (newObj *models.IntellectualObject, err error) {
	if obj == nil {
		return nil, fmt.Errorf("Param obj cannot be nil")
	}
	existingObj, err := client.IntellectualObjectGet(obj.Id, false)
	if err != nil {
		return nil, err
	}

	if client.institutions == nil || len(client.institutions) == 0 {
		err = client.CacheInstitutions()
		if err != nil {
			client.logger.Printf("[ERROR] Fluctus client can't build institutions cache: %v", err)
			return nil, fmt.Errorf("Error building institutions cache: %v", err)
		}
	}

	// Our institution id is the institution's domain name. When we talk to
	// Fluctus, we need to use its institution id.
	fluctusInstitutionId := obj.InstitutionId
	if domainPattern.Match([]byte(obj.InstitutionId)) {
		fluctusInstitutionId = client.institutions[obj.InstitutionId]
	}

	// URL & method for create
	url := client.BuildUrl(fmt.Sprintf("/institutions/%s/objects.json", fluctusInstitutionId))
	method := "POST"
	// URL & method for update
	if existingObj != nil {
		url = client.BuildUrl(fmt.Sprintf("/objects/%s", obj.Id))
		method = "PUT"
	}

	client.logger.Printf("[INFO] About to %s IntellectualObject %s to Fluctus", method, obj.Id)

	data, err := obj.SerializeForFluctus()
	request, err := client.NewJsonRequest(method, url.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 && response.StatusCode != 204 {
		err = fmt.Errorf("Expected status code 201 or 204 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		client.logger.Println("[ERROR]", err)
		return nil, err
	} else {
		client.logger.Printf("[INFO] %s IntellectualObject %s succeeded", method, obj.Id)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		//client.logger.Println(string(body))
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


func (client *Client) GenericFileGet (genericFileId string, includeRelations bool) (*models.GenericFile, error) {
	queryString := ""
	if includeRelations == true {
		queryString = "include_relations=true"
	}
	url := client.BuildUrl(fmt.Sprintf("/files/%s?%s", genericFileId, queryString))
	client.logger.Println("[INFO] Requesting IntellectualObject from fluctus:", url)
	request, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Read the json response
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
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
func (client *Client) GenericFileSave (objId string, gf *models.GenericFile) (newGf *models.GenericFile, err error) {
	existingObj, err := client.GenericFileGet(gf.Id, false)
	if err != nil {
		return nil, err
	}
	// URL & method for create
	url := client.BuildUrl(fmt.Sprintf("/objects/%s/files.json", objId))
	method := "POST"
	// URL & method for update
	if existingObj != nil {
		url = client.BuildUrl(fmt.Sprintf("/files/%s", gf.Id))
		method = "PUT"
	}

	client.logger.Printf("[INFO] About to %s GenericFile %s to Fluctus", method, gf.Identifier)

	data, err := gf.SerializeForFluctus()
	request, err := client.NewJsonRequest(method, url.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 && response.StatusCode != 204 {
		err = fmt.Errorf("Expected status code 201 or 204 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		client.logger.Println("[ERROR]", err)
		return nil, err
	} else {
		client.logger.Printf("[INFO] %s GenericFile %s succeeded", method, gf.Identifier)
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		// client.logger.Println(string(body))
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
func (client *Client) PremisEventSave (objId, objType string, event *models.PremisEvent) (newEvent *models.PremisEvent, err error) {
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
	url := client.BuildUrl(fmt.Sprintf("/files/%s/events.json", objId))
	if objType == "IntellectualObject" {
		url = client.BuildUrl(fmt.Sprintf("/objects/%s/events.json", objId))
	}

	client.logger.Printf("[INFO] Creating %s PremisEvent %s for objId %s", objType, event.EventType, objId)

	data, err := json.Marshal(event)
	request, err := client.NewJsonRequest(method, url.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 201  {
		err = fmt.Errorf("Expected status code 201 but got %d. URL: %s\n",
			response.StatusCode, request.URL)
		client.logger.Println("[ERROR]", err)
		return nil, err
	} else {
		client.logger.Printf("[INFO] %s PremisEvent %s for objId %s succeeded", method, event.EventType, objId)
	}

	// Fluctus should always return the newly created event
	// client.logger.Println(string(body))
	newEvent = &models.PremisEvent{}
	err = json.Unmarshal(body, newEvent)
	if err != nil {
		return nil, err
	}
	return newEvent, nil
}
