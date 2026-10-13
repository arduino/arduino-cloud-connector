// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The body is shaped like arduino-app-cli's AppListResponse, extra fields included.
func TestAppsListsEveryAppWithItsStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/apps" || r.URL.RawQuery != "" {
			t.Errorf("request: got %s %s?%s want GET /v1/apps with no filter", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apps":[`+
			`{"id":"YQ","name":"a","description":"","icon":"","status":"running","example":false,"default":true,"release":false},`+
			`{"id":"Yg","name":"b","description":"","icon":"","status":"stopped","example":true,"default":false,"release":false},`+
			`{"id":"Yw","name":"c","description":"","icon":"","status":"starting","example":false,"default":false,"release":true,"release_id":"c-1"},`+
			`{"id":"ZA","name":"d","description":"","icon":"","example":false,"default":false,"release":false}`+
			`]}`)
	}))
	defer srv.Close()

	apps, err := New(srv.URL).Apps(context.Background())
	if err != nil {
		t.Fatalf("Apps: %v", err)
	}
	want := []App{
		{ID: "YQ", Name: "a", Status: StatusRunning, Default: true},
		{ID: "Yg", Name: "b", Status: StatusStopped},
		{ID: "Yw", Name: "c", Status: StatusStarting},
		{ID: "ZA", Name: "d"},
	}
	if len(apps) != len(want) {
		t.Fatalf("got %d apps, want %d: %+v", len(apps), len(want), apps)
	}
	for i := range want {
		if apps[i] != want[i] {
			t.Errorf("app %d: got %+v want %+v", i, apps[i], want[i])
		}
	}
}

// Active is what decides which Apps a start stops: exactly the two statuses in which
// arduino-app-cli refuses to start another App.
func TestAppActiveIsRunningOrStarting(t *testing.T) {
	for status, want := range map[string]bool{
		StatusRunning: true, StatusStarting: true,
		StatusStopping: false, StatusStopped: false, StatusFailed: false, StatusUninitialized: false, "": false,
	} {
		if got := (App{Status: status}).Active(); got != want {
			t.Errorf("status %q: Active() = %v want %v", status, got, want)
		}
	}
}

func TestAppsUnreachableIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	if _, err := New(url).Apps(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestAppsRefusalIsAFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"details":"unable to find the app"}`)
	}))
	defer srv.Close()
	if _, err := New(srv.URL).Apps(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatalf("got %v, want ErrFailed", err)
	}
}
