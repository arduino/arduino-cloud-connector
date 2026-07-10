// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package identity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// fakeKeystore implements keystoreIface with an in-memory key.
type fakeKeystore struct {
	key *ecdsa.PrivateKey
	pub string
}

func (f *fakeKeystore) ProvisioningPrivateKey() (*ecdsa.PrivateKey, error) { return f.key, nil }
func (f *fakeKeystore) ProvisioningPublicKeyPEM() (string, error)         { return f.pub, nil }

const testUHWID = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"

func TestNewWithUHWID(t *testing.T) {
	svc := NewWithUHWID(testUHWID, &fakeKeystore{})
	if svc.UHWID() != testUHWID {
		t.Errorf("UHWID: got %q want %q", svc.UHWID(), testUHWID)
	}
}

func TestBoardTokenClaims(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	svc := NewWithUHWID(testUHWID, &fakeKeystore{key: key})

	tokenStr, err := svc.BoardToken(key)
	if err != nil {
		t.Fatalf("BoardToken: %v", err)
	}

	parsed, err := jwt.Parse(tokenStr, func(tok *jwt.Token) (any, error) {
		if _, ok := tok.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected signing method %q", tok.Method.Alg())
		}
		return &key.PublicKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse/verify token: %v", err)
	}
	if !parsed.Valid {
		t.Fatal("token reported invalid")
	}

	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		t.Fatalf("unexpected claims type %T", parsed.Claims)
	}

	// The server looks up the board by `iss` (= UHWID) and enforces a window on
	// `iat`; no `exp` is sent.
	if iss, _ := claims["iss"].(string); iss != testUHWID {
		t.Errorf("iss claim: got %q want %q", iss, testUHWID)
	}
	if _, present := claims["iat"]; !present {
		t.Error("iat claim missing")
	}
	if _, present := claims["exp"]; present {
		t.Error("exp claim must NOT be present")
	}
}

func TestBoardTokenSignatureRejectsWrongKey(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	svc := NewWithUHWID(testUHWID, &fakeKeystore{key: key})

	tokenStr, err := svc.BoardToken(key)
	if err != nil {
		t.Fatalf("BoardToken: %v", err)
	}

	_, err = jwt.Parse(tokenStr, func(*jwt.Token) (any, error) {
		return &other.PublicKey, nil // wrong public key
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err == nil {
		t.Fatal("expected verification failure with the wrong public key")
	}
}
