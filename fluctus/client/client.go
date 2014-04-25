// Package client provides a client for the fluctus REST API.
package client

import (
	"net/http"
)

type Client struct {
	hostUrl        string
	apiToken       string
	httpClient     *http.Client
}

func New(hostUrl string, apiToken string) *Client {
	httpClient := new(http.Client)
	return &Client{hostUrl, apiToken, httpClient}
}
