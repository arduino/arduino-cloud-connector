// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package storageapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// These tests cover the HTTP semantics of one ranged request and nothing else:
// what the client asks for, and how it reads what came back. The transfer built on
// top of it — chunk loop, resume, retry budget, verification — is internal/
// downloader's business and is tested there against a scripted fake, so nothing
// here needs to move bytes at scale.

// ── harness ──────────────────────────────────────────────────────────────────

// rangeServer serves a byte slice with real Range support, with knobs for the
// responses a well-behaved server would never send.
type rangeServer struct {
	content []byte

	// status, when non-zero, is returned with no body.
	status int
	// ignoreRange answers 200 with the whole object, like a server with no Range
	// support.
	ignoreRange bool
	// noContentLen suppresses Content-Length (forces chunked encoding).
	noContentLen bool
	// contentRange overrides the Content-Range header on a 206.
	contentRange string

	mu     sync.Mutex
	ranges []string
}

func (s *rangeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.ranges = append(s.ranges, r.Header.Get("Range"))
	s.mu.Unlock()

	if s.status != 0 {
		w.WriteHeader(s.status)
		return
	}

	total := int64(len(s.content))
	if s.ignoreRange {
		if !s.noContentLen {
			w.Header().Set("Content-Length", fmt.Sprint(total))
		}
		w.WriteHeader(http.StatusOK)
		if s.noContentLen {
			// net/http fills in Content-Length itself for a small buffered body, so
			// suppressing the header is not enough: flushing forces chunked encoding,
			// which is what actually leaves the length unknown to the client.
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		_, _ = w.Write(s.content)
		return
	}

	var start, end int64
	if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if end > total-1 {
		end = total - 1
	}
	cr := fmt.Sprintf("bytes %d-%d/%d", start, end, total)
	if s.contentRange != "" {
		cr = s.contentRange
	}
	w.Header().Set("Content-Range", cr)
	w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(s.content[start : end+1])
}

// testClient points a client at srv, bypassing the mTLS material it has no way to
// produce in a unit test.
func testClient(t *testing.T, srv *httptest.Server) *httpClient {
	t.Helper()
	c := newHTTPClient(config.Config{}, nil)
	c.newHTTP = func() (*http.Client, error) { return srv.Client(), nil }
	return c
}

func content(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*31 + 7) % 251)
	}
	return out
}

// ── one ranged request ───────────────────────────────────────────────────────

func TestFetchAppReleaseServesTheRequestedRange(t *testing.T) {
	body := content(5000)
	srv := httptest.NewTLSServer(&rangeServer{content: body})
	defer srv.Close()

	resp, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 1024, 2047)
	if err != nil {
		t.Fatalf("FetchAppRelease: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if !resp.Ranged {
		t.Error("expected Ranged for a 206 response")
	}
	if resp.Start != 1024 {
		t.Errorf("Start: got %d want 1024", resp.Start)
	}
	if resp.Total != int64(len(body)) {
		t.Errorf("Total: got %d want %d", resp.Total, len(body))
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body[1024:2048]) {
		t.Errorf("body: got %d bytes, not the requested range", len(got))
	}
}

// The caller appends the body to a partial file at a known offset, so a response
// that starts elsewhere would leave a hole or a duplicated span. Rejected here, in
// the layer that knows what was asked for, so no caller can forget the check.
func TestFetchAppReleaseRejectsAMisalignedRange(t *testing.T) {
	srv := httptest.NewTLSServer(&rangeServer{
		content:      content(5000),
		contentRange: "bytes 0-1023/5000", // we will ask starting at 1024
	})
	defer srv.Close()

	_, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 1024, 2047)
	if !errors.Is(err, ErrBadHeaders) {
		t.Fatalf("expected ErrBadHeaders, got %v", err)
	}
	if !retryOf(t, err) {
		t.Error("a misaligned range is worth another attempt")
	}
}

func TestFetchAppReleaseRejectsAnUnparsableContentRange(t *testing.T) {
	srv := httptest.NewTLSServer(&rangeServer{
		content:      content(100),
		contentRange: "bytes 0-99/*", // an indefinite total is unusable
	})
	defer srv.Close()

	_, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 0, 99)
	if !errors.Is(err, ErrBadHeaders) {
		t.Fatalf("expected ErrBadHeaders, got %v", err)
	}
}

// A server that ignores Range is reported as such rather than papered over: the
// caller has to know it received the whole object from byte 0, or it would splice
// those bytes onto a partial file.
func TestFetchAppReleaseReportsAnIgnoredRange(t *testing.T) {
	body := content(3000)
	srv := httptest.NewTLSServer(&rangeServer{content: body, ignoreRange: true})
	defer srv.Close()

	resp, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 1024, 2047)
	if err != nil {
		t.Fatalf("FetchAppRelease: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Ranged {
		t.Error("expected Ranged false for a 200 response")
	}
	if resp.Start != 0 {
		t.Errorf("Start: got %d want 0", resp.Start)
	}
	if resp.Total != int64(len(body)) {
		t.Errorf("Total: got %d want %d", resp.Total, len(body))
	}
}

func TestFetchAppReleaseRequiresContentLengthOnAnUnrangedResponse(t *testing.T) {
	// Without a length there is nothing to check free space or the byte count
	// against, and the transfer could never tell "complete" from "truncated".
	srv := httptest.NewTLSServer(&rangeServer{
		content: content(1200), ignoreRange: true, noContentLen: true,
	})
	defer srv.Close()

	_, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 0, 1023)
	if !errors.Is(err, ErrBadHeaders) {
		t.Fatalf("expected ErrBadHeaders, got %v", err)
	}
	if retryOf(t, err) {
		t.Error("a missing Content-Length will not fix itself on a retry")
	}
}

func TestFetchAppReleaseClassifiesStatuses(t *testing.T) {
	// Whether a status is worth retrying is HTTP knowledge, so it is decided here
	// and travels to the retry loop on the Failure.
	cases := []struct {
		status  int
		wantRty bool
	}{
		{http.StatusServiceUnavailable, true},
		{http.StatusInternalServerError, true},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusForbidden, false},  // the grant is gone
		{http.StatusNotFound, false},   // the artefact is gone
		{http.StatusBadRequest, false}, // we asked wrongly
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewTLSServer(&rangeServer{content: content(10), status: tc.status})
			defer srv.Close()

			_, err := testClient(t, srv).FetchAppRelease(context.Background(), srv.URL+"/a/b.zip", 0, 9)
			if !errors.Is(err, ErrBadResponse) {
				t.Fatalf("expected ErrBadResponse, got %v", err)
			}
			if got := retryOf(t, err); got != tc.wantRty {
				t.Errorf("Retry: got %v want %v", got, tc.wantRty)
			}
		})
	}
}

func TestFetchAppReleaseRejectsANonsensicalRange(t *testing.T) {
	c := newHTTPClient(config.Config{}, nil)
	for _, tc := range []struct{ start, end int64 }{{-1, 10}, {10, 5}} {
		if _, err := c.FetchAppRelease(context.Background(), "https://h/x.zip", tc.start, tc.end); !errors.Is(err, ErrURLInvalid) {
			t.Errorf("range %d-%d: expected ErrURLInvalid, got %v", tc.start, tc.end, err)
		}
	}
}

// A body the caller never receives has to be closed here, or every rejected
// response leaks a connection for the life of the daemon.
func TestFetchAppReleaseClosesTheBodyItRejects(t *testing.T) {
	body := &trackedBody{Reader: io.LimitReader(rand.Reader, 10)}
	c := newHTTPClient(config.Config{}, nil)
	c.newHTTP = func() (*http.Client, error) {
		return &http.Client{Transport: stubTransport{resp: &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
			Body:       body,
			Header:     http.Header{},
		}}}, nil
	}

	if _, err := c.FetchAppRelease(context.Background(), "https://h/x.zip", 0, 9); err == nil {
		t.Fatal("expected the 403 to fail")
	}
	if !body.closed {
		t.Error("the rejected response body was left open")
	}
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type stubTransport struct{ resp *http.Response }

func (s stubTransport) RoundTrip(*http.Request) (*http.Response, error) { return s.resp, nil }

// retryOf reads the retry decision off the Failure, the way the download loop does.
func retryOf(t *testing.T, err error) bool {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error is not a *Failure: %v", err)
	}
	return f.Retry
}

// ── URL and TLS ──────────────────────────────────────────────────────────────

func TestValidateURL(t *testing.T) {
	c := newHTTPClient(config.Config{}, nil)
	cases := []struct {
		name     string
		url      string
		sentinel error
	}{
		{"storage host", "https://api2.arduino.cc/storage/app/v1/x.zip", nil},
		{"host with port", "https://api2.arduino.cc:443/x.zip", nil},
		// Any https host is accepted: the cloud owns the URL, exactly as in the C++
		// library, so a CDN or a bucket on another domain must not be rejected.
		{"other host", "https://cdn.example/x.zip", nil},
		{"plain http", "http://api2.arduino.cc/x.zip", ErrURLInvalid},
		{"no host", "https:///x.zip", ErrURLInvalid},
		{"unparsable", "https://%zz", ErrURLInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.validateURL(tc.url)
			if tc.sentinel == nil {
				if err != nil {
					t.Fatalf("expected %s to be accepted, got %v", tc.url, err)
				}
				return
			}
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("expected %v for %s, got %v", tc.sentinel, tc.url, err)
			}
			if retryOf(t, err) {
				t.Error("URL validation failures must not be retryable")
			}
		})
	}
}

// TestDeviceHTTPClientRootCAs pins the trust rule the storage client shares with
// the broker client: MQTT_CA_FILE, when set, is the sole trusted root here too.
// Getting this wrong is invisible until a live handshake against a pinned
// environment fails, so it is asserted on the built tls.Config.
func TestDeviceHTTPClientRootCAs(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, selfSignedCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	ks := newFakeCertProvider(t)

	rootsOf := func(t *testing.T, cfg config.Config) *x509.CertPool {
		t.Helper()
		hc, err := newHTTPClient(cfg, ks).deviceHTTPClient()
		if err != nil {
			t.Fatalf("deviceHTTPClient: %v", err)
		}
		tr, ok := hc.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("transport is %T, want *http.Transport", hc.Transport)
		}
		if len(tr.TLSClientConfig.Certificates) != 1 {
			t.Error("expected the device certificate to be presented for mTLS")
		}
		return tr.TLSClientConfig.RootCAs
	}

	t.Run("unset CA file uses system roots", func(t *testing.T) {
		if pool := rootsOf(t, config.Config{}); pool != nil {
			t.Error("expected nil RootCAs (system roots)")
		}
	})

	t.Run("pinned CA file is used for storage too", func(t *testing.T) {
		if pool := rootsOf(t, config.Config{MQTTCAFile: caFile}); pool == nil {
			t.Error("expected RootCAs to be the pinned pool, got nil (system roots)")
		}
	})

	t.Run("unreadable CA file fails the client build", func(t *testing.T) {
		cfg := config.Config{MQTTCAFile: filepath.Join(t.TempDir(), "absent.pem")}
		if _, err := newHTTPClient(cfg, ks).deviceHTTPClient(); err == nil {
			t.Error("expected an error rather than a silent fallback to system roots")
		}
	})
}

// fakeCertProvider hands out a throwaway EC key and a matching self-signed
// certificate, which is all tls.X509KeyPair needs to build the mTLS material.
type fakeCertProvider struct {
	key     *ecdsa.PrivateKey
	certPEM string
}

func newFakeCertProvider(t *testing.T) *fakeCertProvider {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-device"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeCertProvider{key: key, certPEM: string(pem.EncodeToMemory(
		&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

func (f *fakeCertProvider) CloudPrivateKey() (*ecdsa.PrivateKey, error) { return f.key, nil }
func (f *fakeCertProvider) DeviceCertPEM() (string, error)              { return f.certPEM, nil }

// selfSignedCAPEM returns a throwaway self-signed CA certificate in PEM form.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ── parsing and error carrier ────────────────────────────────────────────────

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in          string
		start, tot  int64
		expectError bool
	}{
		{in: "bytes 0-1023/8000", start: 0, tot: 8000},
		{in: "bytes 4096-4999/5000", start: 4096, tot: 5000},
		{in: " bytes 10-20/30 ", start: 10, tot: 30},
		{in: "bytes 0-1023/*", expectError: true},
		{in: "items 0-10/20", expectError: true},
		{in: "bytes 0-1023", expectError: true},
		{in: "bytes /8000", expectError: true},
		{in: "", expectError: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			start, total, err := parseContentRange(tc.in)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected an error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseContentRange(%q): %v", tc.in, err)
			}
			if start != tc.start || total != tc.tot {
				t.Errorf("got (%d,%d) want (%d,%d)", start, total, tc.start, tc.tot)
			}
		})
	}
}

// TestFailureUnwrapsToSentinelAndCause guards the contract callers rely on: every
// failure is classifiable with errors.Is against an exported sentinel, while the
// underlying cause stays reachable for diagnosis.
func TestFailureUnwrapsToSentinelAndCause(t *testing.T) {
	cause := errors.New("connection reset")
	err := Retryable(ErrConnect, "GET artefact").Wrap(cause)

	if !errors.Is(err, ErrConnect) {
		t.Error("errors.Is did not match the sentinel")
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is did not match the wrapped cause")
	}
	if errors.Is(err, ErrBadHeaders) {
		t.Error("errors.Is matched an unrelated sentinel")
	}
	if !retryOf(t, err) {
		t.Error("Retryable() produced a non-retryable failure")
	}
	if retryOf(t, Terminal(ErrBadResponse, "nope")) {
		t.Error("Terminal() produced a retryable failure")
	}
}
