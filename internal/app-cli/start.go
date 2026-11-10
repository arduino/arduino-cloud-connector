// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Start starts the App appID and follows the start to its verdict (RFC-16 §5,
// POST /v1/apps/{appID}/start).
//
// arduino-app-cli refuses to start an App while another one is running or starting
// (ErrAnotherAppRunning) — the App itself included — so a caller that wants this App
// to run whatever else is up stops the others first. The start has no done event: it
// succeeded when the stream closes with no error before it.
func (c *Client) Start(ctx context.Context, appID string, onProgress ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/apps/"+url.PathEscape(appID)+"/start", http.NoBody)
	if err != nil {
		return fmt.Errorf("%w: build the request: %v", ErrFailed, err)
	}
	return c.stream(ctx, req, onProgress, untilClosed, lifecycleRefused)
}

// lifecycleRefused classifies a refusal of a start or stop. Their unprefixed server
// error is "unable to find the app" as often as a stream that could not be opened,
// so it is a failure of the command, not arduino-app-cli being unreachable.
func lifecycleRefused(resp *http.Response) error { return statusError(resp, ErrFailed) }
