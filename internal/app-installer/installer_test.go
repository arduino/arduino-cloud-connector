// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appinstaller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRequestWireShape(t *testing.T) {
	// The handover body is this package's contract with arduino-app-cli: exactly two
	// fields, with exactly these names. A field silently reappearing — cloud_app_id
	// and name were both in an RFC draft — or being renamed fails here rather than at
	// app-cli, which would only notice at deploy time on a real board.
	blob, err := json.Marshal(Request{BundlePath: "/var/lib/x/job.zip", SHA256: "deadbeef"})
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]any
	if err := json.Unmarshal(blob, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Errorf("handover body has %d fields, want 2: %s", len(fields), blob)
	}
	for _, key := range []string{"bundle_path", "sha256"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("handover body is missing %q: %s", key, blob)
		}
	}
}

func TestUnavailableFailsWithErrUnavailable(t *testing.T) {
	// The placeholder must fail in a way the caller can classify, because that is what
	// turns into the wire code the operator sees. An untyped error would be reported as
	// an internal fault instead of "no installer".
	err := Unavailable().Install(context.Background(), Request{BundlePath: "/x.zip"}, nil)
	if err == nil {
		t.Fatal("Unavailable installer reported success")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("error does not match ErrUnavailable: %v", err)
	}
	// It must NOT masquerade as an install that started and then failed — the bundle
	// is untouched and the two map to different Cloud error codes.
	if errors.Is(err, ErrFailed) {
		t.Errorf("an installer that was never reached reported ErrFailed: %v", err)
	}
}

// The four specific verdicts refine ErrFailed rather than replacing it, so both
// questions a caller can ask keep working: "did the install succeed?" and "what
// exactly did arduino-app-cli object to?".
//
// Which matters because internal/ota classifies specific-first: if one of these ever
// stopped matching ErrFailed, an unhandled verdict would fall through to the deploy
// phase's fallback instead of being reported as an install failure at all.
func TestSpecificVerdictsRefineErrFailed(t *testing.T) {
	verdicts := map[string]error{
		"archive rejected": ErrArchiveRejected,
		"invalid app.yaml": ErrInvalidAppYaml,
		"not compatible":   ErrNotCompatible,
		"does not run":     ErrRunFailed,
	}
	for name, verdict := range verdicts {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(verdict, ErrFailed) {
				t.Errorf("%v does not match ErrFailed", verdict)
			}
			// The install did start, so it is not the "no installer" case.
			if errors.Is(verdict, ErrUnavailable) {
				t.Errorf("%v matches ErrUnavailable", verdict)
			}
			// And they stay distinguishable from one another, which is the whole
			// reason they exist: each is a different code on the operator's screen.
			for otherName, other := range verdicts {
				if otherName != name && errors.Is(verdict, other) {
					t.Errorf("%s is indistinguishable from %s", name, otherName)
				}
			}
		})
	}
	// The generic sentinel must NOT match the specific ones, or every unqualified
	// install failure would be reported as whichever one happened to be checked first.
	for name, verdict := range verdicts {
		if errors.Is(ErrFailed, verdict) {
			t.Errorf("a plain ErrFailed matches %s", name)
		}
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
