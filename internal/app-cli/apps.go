// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// App statuses, as arduino-app-cli reports them.
const (
	StatusStarting      = "starting"
	StatusRunning       = "running"
	StatusStopping      = "stopping"
	StatusStopped       = "stopped"
	StatusFailed        = "failed"
	StatusUninitialized = "uninitialized"
)

// App is one App installed on the board, as GET /v1/apps lists it.
type App struct {
	// ID is arduino-app-cli's id of the App, the one its endpoints take.
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	// Default is the App the board starts at boot.
	Default bool `json:"default"`
}

// Active reports whether the App is running or starting — the two statuses in which
// arduino-app-cli refuses to start another App.
func (a App) Active() bool { return a.Status == StatusRunning || a.Status == StatusStarting }

// maxListBody bounds the App list response. A board holds tens of Apps, not
// thousands.
const maxListBody = 4 << 20

// Apps lists every App on the board — user Apps, examples and releases alike, since
// any of them can be the one running (GET /v1/apps).
func (c *Client) Apps(ctx context.Context) ([]App, error) {
	resp, done, err := c.request(ctx, http.MethodGet, "/v1/apps", http.NoBody, "")
	if err != nil {
		return nil, err
	}
	defer done()
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp, ErrFailed)
	}

	var list struct {
		Apps []App `json:"apps"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxListBody)).Decode(&list); err != nil {
		return nil, readFailed(ctx, resp, "the app list", err)
	}
	return list.Apps, nil
}
