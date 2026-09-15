// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// The cross-check between the two independent encoders.
//
// This is the ONE file in the harness allowed to import the daemon (see the
// depguard exemption in test/e2e/.golangci.yml), because comparing the two
// implementations is by definition something only a file that sees both can
// do. Everything else in the harness reimplements what it needs.
//
// What it buys: the harness CA signs a certificate, the REAL provisioning
// client fetches the pipe-delimited response over HTTP and reconstructs its
// own copy, and the two DERs must be identical. When they are not, the failure
// says WHICH side changed and where -- instead of surfacing days later as an
// "unknown certificate authority" from a broker.
//
// It also goes through the whole external boundary rather than calling the
// encoder directly: the fake HTTP server, the JSON request body, the plain-text
// response, the client's own parsing. A break anywhere on that path is a break
// the E2E suite would hit.
package pki_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	provisioningapi "github.com/arduino/arduino-cloud-connector/internal/provisioning-api"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/provapi"
)

const (
	testUHWID    = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"
	testDeviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
)

// newFake starts the harness CA and the fake API in front of it.
func newFake(t *testing.T, directives map[provapi.Endpoint][]provapi.Directive) (*pki.CA, *provapi.Server) {
	t.Helper()
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	srv, err := provapi.Start(provapi.Options{
		Log:        eventlog.New(),
		CA:         ca,
		DeviceID:   testDeviceID,
		Directives: directives,
	})
	if err != nil {
		t.Fatalf("start fake API: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return ca, srv
}

// newCSR builds a CSR the way keystore.GenerateCSR does, with an optional
// pollution of the subject.
func newCSR(t *testing.T, subject pkix.Name) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("device key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: subject}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))
}

func newClient(srv *provapi.Server) provisioningapi.Client {
	return provisioningapi.NewClient(config.Config{ProvisioningAPI: srv.URL()})
}

// The cross-check itself: two independently written encoders, one certificate,
// byte-for-byte equality.
func TestHarnessIssuedCertificateMatchesTheDaemonReconstruction(t *testing.T) {
	_, srv := newFake(t, nil)
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	deviceID, certPEM, err := newClient(srv).SubmitCSR(ctx, "board-token", csrPEM)
	if err != nil {
		t.Fatalf("the daemon client could not use the fake response: %v", err)
	}
	if deviceID != testDeviceID {
		t.Errorf("device id = %q, want %q", deviceID, testDeviceID)
	}

	issued, ok := srv.Issued()
	if !ok {
		t.Fatal("the fake API reports no issued certificate")
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("the reconstructed certificate is not valid PEM")
	}

	if string(block.Bytes) != string(issued.CertDER) {
		t.Errorf("the two encoders disagree on the certificate DER\n"+
			"  harness (cryptobyte):   %x\n"+
			"  daemon  (reconstructed): %x\n"+
			"One of the two changed. Compare field by field: issuer DN, validity "+
			"encoding, subject, public key, the authority key identifier "+
			"extension and the signature wrapper.",
			issued.CertDER, block.Bytes)
	}
}

// The signature is what a broker actually checks, and it only verifies if the
// daemon reconstructed exactly the bytes the CA signed. This asserts the
// consequence, not just the byte equality above: the same failure the broker
// would report, caught here.
func TestReconstructedCertificateVerifiesAgainstTheHarnessCA(t *testing.T) {
	ca, srv := newFake(t, nil)
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, certPEM, err := newClient(srv).SubmitCSR(ctx, "board-token", csrPEM)
	if err != nil {
		t.Fatalf("submit CSR: %v", err)
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("the reconstructed certificate is not valid PEM")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("strict parse of the reconstructed certificate: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       ca.Pool(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime: time.Now(),
	}); err != nil {
		t.Fatalf("chain verify for client auth: %v", err)
	}
}

// The regression this whole design exists for.
//
// A CSR polluted with an extra attribute -- a stray Country=IT was the real
// bug -- gets that attribute reflected into the signed certificate, while the
// daemon rebuilds a CN-only subject. The reconstruction therefore diverges and
// the signature stops verifying: exactly the "unknown certificate authority"
// the broker reported, reproduced here at the point where it is explainable.
//
// If this test ever passes with a polluted CSR, the fake CA has stopped
// reflecting and that entire bug class has gone invisible to the E2E suite.
func TestPollutedCSRSubjectBreaksTheDaemonReconstruction(t *testing.T) {
	ca, srv := newFake(t, nil)
	polluted := newCSR(t, pkix.Name{
		Country:    []string{"IT"},
		CommonName: testUHWID,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// The daemon is happy: nothing in the response tells it anything is wrong.
	_, certPEM, err := newClient(srv).SubmitCSR(ctx, "board-token", polluted)
	if err != nil {
		t.Fatalf("submit CSR: %v", err)
	}
	issued, ok := srv.Issued()
	if !ok {
		t.Fatal("the fake API reports no issued certificate")
	}
	if issued.Subject.Country != "IT" {
		t.Fatalf("the fake CA did not reflect the CSR country: issued subject = %s", issued.Subject)
	}

	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		t.Fatal("the reconstructed certificate is not valid PEM")
	}
	if string(block.Bytes) == string(issued.CertDER) {
		t.Fatal("the reconstruction matched a certificate signed over a polluted subject, " +
			"so a CSR carrying extra attributes would go unnoticed")
	}

	// The reconstruction is internally well-formed -- the daemon computes its
	// own lengths -- so it parses. What it is not is what the CA signed, and
	// that is where the failure lands.
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("the reconstructed certificate should still be well-formed DER: %v", err)
	}
	if err := leaf.CheckSignatureFrom(ca.Certificate()); err == nil {
		t.Error("the reconstructed certificate still verified against the CA, " +
			"so the polluted CSR would reach the broker as a working certificate")
	}
}

// The malformed directive has to trip the daemon's own parser, not merely look
// wrong: this pins the fake fault against the real error path.
func TestMalformedResponseIsRejectedByTheDaemonClient(t *testing.T) {
	_, srv := newFake(t, map[provapi.Endpoint][]provapi.Directive{
		provapi.EndpointCSR: {{Respond: provapi.RespondMalformed}},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, _, err := newClient(srv).SubmitCSR(ctx, "board-token", csrPEM); err == nil {
		t.Fatal("the daemon client accepted a malformed CSR response")
	}
}

// A queued 503 has to reach the daemon as a failure, which is what makes the
// retry scenarios meaningful. The client itself does not retry -- the
// provisioning service above it does -- so one call, one error.
func TestQueuedStatusReachesTheDaemonClientAsAnError(t *testing.T) {
	_, srv := newFake(t, map[provapi.Endpoint][]provapi.Directive{
		provapi.EndpointCSR:      {{Respond: provapi.RespondStatus, Status: 503}},
		provapi.EndpointComplete: {{Respond: provapi.RespondStatus, Status: 500}},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := newClient(srv)

	if _, _, err := client.SubmitCSR(ctx, "board-token", csrPEM); err == nil {
		t.Error("a 503 from provision/csr was not reported as an error")
	}
	if err := client.Complete(ctx, "board-token"); err == nil {
		t.Error("a 500 from provision/complete was not reported as an error")
	}
	// Queues are exhausted now, so the happy defaults take over: the same fake
	// must be able to carry a scenario from failure into success.
	if _, _, err := client.SubmitCSR(ctx, "board-token", csrPEM); err != nil {
		t.Errorf("the retry after the queued 503 failed: %v", err)
	}
	if err := client.Complete(ctx, "board-token"); err != nil {
		t.Errorf("the retry after the queued 500 failed: %v", err)
	}
}
