// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appinstaller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
)

func TestUnavailableFailsWithErrUnavailable(t *testing.T) {
	// The placeholder must fail in a way the caller can classify, because that is what
	// turns into the wire code the operator sees. An untyped error would be reported as
	// an internal fault instead of "no installer".
	err := Unavailable().Install(context.Background(), Request{BundlePath: "/x.zip"}, nil)
	if err == nil {
		t.Fatal("Unavailable installer reported success")
	}
	if !errors.Is(err, appcli.ErrUnavailable) {
		t.Errorf("error does not match ErrUnavailable: %v", err)
	}
	// It must NOT masquerade as an install that started and then failed — the bundle
	// is untouched and the two map to different Cloud error codes.
	if errors.Is(err, appcli.ErrFailed) {
		t.Errorf("an installer that was never reached reported ErrFailed: %v", err)
	}
}

func TestFuncAdaptsToInstaller(t *testing.T) {
	// Func is what every test double in internal/ota is built from, so its plumbing
	// (request through, progress callback through) is worth one direct test.
	var got Request
	var progress []int32

	var inst Installer = Func(func(_ context.Context, req Request, onProgress ProgressFunc) error {
		got = req
		onProgress(10)
		onProgress(100)
		return nil
	})

	want := Request{BundlePath: "/var/lib/x/job.zip", SHA256: "abc"}
	if err := inst.Install(context.Background(), want, func(p int32) {
		progress = append(progress, p)
	}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if got != want {
		t.Errorf("request: got %+v want %+v", got, want)
	}
	if len(progress) != 2 || progress[0] != 10 || progress[1] != 100 {
		t.Errorf("progress forwarded: got %v want [10 100]", progress)
	}
}

// The adapter hands the bundle path to arduino-app-cli and its progress back, on
// Install's own goroutine — which the real client guarantees by reading the stream
// there.
func TestAppCLIAdapterInstallsThroughTheClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("no bundle uploaded: %v", err)
		}
		defer func() { _ = file.Close() }()
		if body, _ := io.ReadAll(file); string(body) != "bundle" || header.Filename != "job.zip" {
			t.Errorf("uploaded %q as %q", body, header.Filename)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: progress\ndata: {\"progress\":40}\n\n"+
			"event: done\ndata: {}\n\n")
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "job.zip")
	if err := os.WriteFile(path, []byte("bundle"), 0o600); err != nil {
		t.Fatal(err)
	}
	var seen []int32
	err := AppCLI(appcli.New(srv.URL)).Install(context.Background(), Request{BundlePath: path},
		func(p int32) { seen = append(seen, p) })
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !slices.Equal(seen, []int32{40}) {
		t.Errorf("progress: got %v want [40]", seen)
	}
}
