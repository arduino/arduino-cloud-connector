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

// Stop stops the App appID and follows the stop to its verdict (RFC-16 §5,
// POST /v1/apps/{appID}/stop). Like the start it has no done event: it succeeded
// when the stream closes with no error before it.
func (c *Client) Stop(ctx context.Context, appID string, onProgress ProgressFunc) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/apps/"+url.PathEscape(appID)+"/stop", http.NoBody)
	if err != nil {
		return fmt.Errorf("%w: build the request: %v", ErrFailed, err)
	}
	return c.stream(ctx, req, onProgress, untilClosed, lifecycleRefused)
}
