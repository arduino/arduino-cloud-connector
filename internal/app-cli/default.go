// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// SetDefault makes appID the App the board starts at boot (RFC-16 §5,
// PATCH /v1/apps/{appID} with {"default": true}).
//
// arduino-app-cli keeps a single default and replaces it atomically, so setting a new
// one needs nothing done to the previous one first. The call is a plain request: it
// answers with the App's details, which are not needed here.
func (c *Client) SetDefault(ctx context.Context, appID string) error {
	resp, done, err := c.request(ctx, http.MethodPatch, "/v1/apps/"+url.PathEscape(appID),
		strings.NewReader(`{"default":true}`), "application/json")
	if err != nil {
		return err
	}
	defer done()
	if resp.StatusCode != http.StatusOK {
		return statusError(resp, ErrFailed)
	}
	return nil
}
