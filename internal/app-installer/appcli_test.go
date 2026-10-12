// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appinstaller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The event streams below are written out by hand, byte for byte as arduino-app-cli's
// render/sse.go writes them ("event: x\ndata: <json>\n\n"), rather than produced by
// any encoder of ours: a test that parses what its own code wrote would agree with
// itself whatever the wire looked like.

// streamClosed is the tail of every arduino-app-cli stream, success included.
const streamClosed = "event: error\ndata: {\"code\":\"SERVER_CLOSED\"}\n\n" +
	"event: close\ndata: \"Stream closed by server\"\n\n"

func writeBundleFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "0123abcd.zip")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// sseServer answers the install request with stream, once the upload is read.
func sseServer(t *testing.T, stream string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := r.FormFile("file"); err != nil {
			t.Errorf("the request carries no multipart file: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func install(t *testing.T, srv *httptest.Server, onProgress ProgressFunc) error {
	t.Helper()
	if onProgress == nil {
		onProgress = func(int32) {}
	}
	req := Request{BundlePath: writeBundleFile(t, []byte("bundle")), SHA256: "00"}
	return NewAppCLI(srv.URL).Install(context.Background(), req, onProgress)
}

// The request is the contract with arduino-app-cli's handler: PUT on the install
// path, prepare=true (it defaults to false, and a deploy without it would leave an App
// that cannot start), and the bundle — every byte of it — in the multipart field
// "file".
func TestAppCLIUploadsTheBundleAsMultipart(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB: several pipe writes
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/apps/install" {
			t.Errorf("request: got %s %s want PUT /v1/apps/install", r.Method, r.URL.Path)
		}
		if p := r.URL.Query().Get("prepare"); p != "true" {
			t.Errorf("prepare: got %q want \"true\"", p)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("FormFile(file): %v", err)
		}
		defer file.Close()
		if header.Filename != "0123abcd.zip" {
			t.Errorf("file name: got %q", header.Filename)
		}
		got, _ = io.ReadAll(file)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: done\ndata: {\"id\":\"x\",\"name\":\"a\",\"release\":\"a-1\",\"target\":\"unoq\"}\n\n"+streamClosed)
	}))
	defer srv.Close()

	req := Request{BundlePath: writeBundleFile(t, content)}
	if err := NewAppCLI(srv.URL+"/").Install(context.Background(), req, func(int32) {}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("uploaded %d bytes, want the %d of the bundle, identical", len(got), len(content))
	}
}

// A successful install, shaped like a real one: a heartbeat during phase 0, integral
// percentages from the container pull, fractional ones from the models, progress:100
// BEFORE done, and the SERVER_CLOSED error after it — which must not turn a success
// into a failure.
func TestAppCLIFollowsASuccessfulInstall(t *testing.T) {
	stream := "event: heartbeat\n\n" +
		"event: message\ndata: {\"message\":\"downloading the containers\"}\n\n" +
		"event: progress\ndata: {\"name\":\"containers\",\"progress\":0}\n\n" +
		"event: progress\ndata: {\"name\":\"Pulling image 1/1 (img)\",\"progress\":25}\n\n" +
		"event: progress\ndata: {\"name\":\"Pulling image 1/1 (img)\",\"progress\":25}\n\n" +
		"event: progress\ndata: {\"name\":\"Pulling image 1/1 (img)\",\"progress\":50}\n\n" +
		"event: progress\ndata: {\"name\":\"models\",\"progress\":66.66667}\n\n" +
		"event: progress\ndata: {\"name\":\"models\",\"progress\":83.33333}\n\n" +
		"event: progress\ndata: {\"name\":\"\",\"progress\":100}\n\n" +
		"event: done\ndata: {\"id\":\"cmVsZWFzZTphLTE\",\"name\":\"a\",\"release\":\"a-1\",\"target\":\"unoq\"}\n\n" +
		streamClosed

	var seen []int32
	if err := install(t, sseServer(t, stream), func(p int32) { seen = append(seen, p) }); err != nil {
		t.Fatalf("Install: %v", err)
	}
	// Every progress event is forwarded, repeats included: each one is a sign of life
	// for the caller's watchdog, and deduplicating is the caller's business.
	want := []int32{0, 25, 25, 50, 66, 83, 100}
	if !slices.Equal(seen, want) {
		t.Errorf("progress: got %v want %v", seen, want)
	}
}

// The Cloud's progress bar must never go backwards, and never past 100.
func TestAppCLIProgressIsMonotonicAndCapped(t *testing.T) {
	stream := "event: progress\ndata: {\"progress\":40}\n\n" +
		"event: progress\ndata: {\"progress\":10}\n\n" +
		"event: progress\ndata: {\"progress\":250}\n\n" +
		"event: done\ndata: {}\n\n"
	var seen []int32
	if err := install(t, sseServer(t, stream), func(p int32) { seen = append(seen, p) }); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if want := []int32{40, 40, 100}; !slices.Equal(seen, want) {
		t.Errorf("progress: got %v want %v", seen, want)
	}
}

// Every in-stream failure of RFC-14 §6.1, as arduino-app-cli sends it once the
// prefixes are in: the code its render package sets, and the message with the prefix
// in front. Each must reach the caller as its own sentinel, because each is its own
// Cloud error code.
func TestAppCLIClassifiesStreamErrors(t *testing.T) {
	cases := []struct {
		name, code, message string
		want                error
	}{
		{"not gzip", "bad_request",
			"AppArchiveRejected: bad request: app-install-1.tar.gz is not a release archive: gzip: invalid header", ErrArchiveRejected},
		{"no manifest", "bad_request",
			"AppYamlInvalid: bad request: x: it carries no release manifest, it is not a release", ErrInvalidAppYaml},
		{"other board", "bad_request",
			"AppNotCompatible: bad request: the release is built for X, this board is a Y", ErrNotCompatible},
		{"already installed", "app_already_exists",
			"app already exists: a-1 is already installed", ErrAlreadyExists},
		{"extraction", "INTERNAL_SERVER_ERROR",
			"ExtractionError: failed to extract a-1: no space left on device", ErrExtraction},
		// The longer prefix must not be mistaken for the shorter one it starts with.
		{"resources, disk full", "INTERNAL_SERVER_ERROR",
			"AdditionalResourceDownloadErrorNoDiskSpace: a-1 is installed, but failed to download the containers of the release: not enough disk space to pull the docker image",
			ErrResourceNoSpace},
		{"resources, other", "INTERNAL_SERVER_ERROR",
			"AdditionalResourceDownloadError: a-1 is installed, but failed to download the model \"m\": timeout", ErrResourceDownload},
		{"generic", "INTERNAL_SERVER_ERROR",
			"GenericError: failed to install the release: rename", ErrFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := "event: progress\ndata: {\"progress\":0}\n\n" +
				fmt.Sprintf("event: error\ndata: {\"code\":%q,\"message\":%q}\n\n", tc.code, tc.message) +
				streamClosed
			err := install(t, sseServer(t, stream), nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			assertOnlySpecific(t, err, tc.want)
			// The message is what the operator log gets; it must survive.
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("the error lost arduino-app-cli's message: %v", err)
			}
		})
	}
}

// Without a prefix there is nothing finer to say than "the install failed" — including
// for a bad_request, which lumps three Cloud codes together.
func TestAppCLIUnprefixedStreamErrorIsAGenericFailure(t *testing.T) {
	for _, code := range []string{"bad_request", "INTERNAL_SERVER_ERROR", "something_new"} {
		t.Run(code, func(t *testing.T) {
			stream := fmt.Sprintf("event: error\ndata: {\"code\":%q,\"message\":\"bad request: x is empty\"}\n\n", code) + streamClosed
			err := install(t, sseServer(t, stream), nil)
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("got %v, want ErrFailed", err)
			}
			assertOnlySpecific(t, err, ErrFailed)
		})
	}
}

// A stream that ends before a verdict leaves the outcome unknown: RFC-14 §5.10 -41.
// SERVER_CLOSED arriving first is the ordinary way that happens, a bare close or a
// cut connection are the others.
func TestAppCLIStreamWithoutAVerdictIsUnavailable(t *testing.T) {
	progress := "event: progress\ndata: {\"progress\":10}\n\n"
	for name, stream := range map[string]string{
		"server closed": progress + streamClosed,
		"close only":    progress + "event: close\ndata: \"Stream closed by server\"\n\n",
		"cut":           progress,
		"cut mid-event": progress + "event: done\n",
	} {
		t.Run(name, func(t *testing.T) {
			err := install(t, sseServer(t, stream), nil)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("got %v, want ErrUnavailable", err)
			}
			if errors.Is(err, ErrFailed) {
				t.Errorf("an unknown outcome was reported as a failed install: %v", err)
			}
		})
	}
}

// Refusals before the stream opens: {"details": ...} with an HTTP status.
func TestAppCLIClassifiesHTTPRefusals(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		details string
		want    error
	}{
		{"missing file", http.StatusBadRequest, "missing required file parameter", ErrFailed},
		{"temp file", http.StatusInternalServerError, "GenericError: internal server error", ErrFailed},
		{"disk full on upload", http.StatusInternalServerError, "NoDiskSpace: failed to save uploaded file", ErrNoSpace},
		{"no sse stream", http.StatusInternalServerError, "unable to create SSE stream", ErrUnavailable},
		// An arduino-app-cli without the endpoint is too old to deploy the App.
		{"no endpoint", http.StatusNotFound, "", ErrNotCompatible},
		{"old method", http.StatusMethodNotAllowed, "", ErrNotCompatible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprintf(w, "{\"details\":%q}\n", tc.details)
			}))
			defer srv.Close()
			err := install(t, srv, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			assertOnlySpecific(t, err, tc.want)
		})
	}
}

func TestAppCLIUnreachableIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there any more

	req := Request{BundlePath: writeBundleFile(t, []byte("bundle"))}
	err := NewAppCLI(url).Install(context.Background(), req, func(int32) {})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

// A bundle that cannot be read is our failure, not arduino-app-cli's absence.
func TestAppCLIMissingBundleIsAFailedInstall(t *testing.T) {
	req := Request{BundlePath: filepath.Join(t.TempDir(), "gone.zip")}
	err := NewAppCLI("http://127.0.0.1:1").Install(context.Background(), req, func(int32) {})
	if !errors.Is(err, ErrFailed) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrFailed and not ErrUnavailable", err)
	}
}

// Cancellation — a shutdown, or the caller's stall watchdog — is returned as-is, and
// no progress is reported after it: the caller's state machine has moved on.
func TestAppCLICancellationIsReturnedAsIs(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, _ = r.FormFile("file")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: progress\ndata: {\"progress\":10}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	var after []int32
	cancelled := false
	req := Request{BundlePath: writeBundleFile(t, []byte("bundle"))}

	done := make(chan error, 1)
	go func() {
		done <- NewAppCLI(srv.URL).Install(ctx, req, func(p int32) {
			if cancelled {
				after = append(after, p)
			}
			cancelled = true
			cancel()
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Install did not return after cancellation")
	}
	if len(after) > 0 {
		t.Errorf("progress reported after cancellation: %v", after)
	}
}

// assertOnlySpecific checks err matches no specific verdict other than want, so a
// classification cannot pass by matching two sentinels at once.
func assertOnlySpecific(t *testing.T, err, want error) {
	t.Helper()
	for _, other := range []error{
		ErrUnavailable, ErrNoSpace, ErrArchiveRejected, ErrInvalidAppYaml, ErrNotCompatible,
		ErrAlreadyExists, ErrExtraction, ErrResourceNoSpace, ErrResourceDownload,
	} {
		if other != want && errors.Is(err, other) {
			t.Errorf("%v also matches %v", err, other)
		}
	}
}
