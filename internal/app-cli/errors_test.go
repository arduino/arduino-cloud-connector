// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"errors"
	"testing"
)

// The specific verdicts refine ErrFailed rather than replacing it, so both
// questions a caller can ask keep working: "did the operation succeed?" and "what
// exactly did arduino-app-cli object to?".
//
// Which matters because the callers classify specific-first: if one of these ever
// stopped matching ErrFailed, an unhandled verdict would fall through to the caller's
// fallback instead of being reported as a failure of the operation at all.
func TestSpecificVerdictsRefineErrFailed(t *testing.T) {
	verdicts := map[string]error{
		"no space":            ErrNoSpace,
		"not compatible":      ErrNotCompatible,
		"archive rejected":    ErrArchiveRejected,
		"invalid app.yaml":    ErrInvalidAppYaml,
		"already exists":      ErrAlreadyExists,
		"extraction":          ErrExtraction,
		"resources: no space": ErrResourceNoSpace,
		"resources: download": ErrResourceDownload,
		"not prepared":        ErrNotPrepared,
		"another app running": ErrAnotherAppRunning,
	}
	for name, verdict := range verdicts {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(verdict, ErrFailed) {
				t.Errorf("%v does not match ErrFailed", verdict)
			}
			// The operation did start, so it is not the "unreachable" case.
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
	// failure would be reported as whichever one happened to be checked first.
	for name, verdict := range verdicts {
		if errors.Is(ErrFailed, verdict) {
			t.Errorf("a plain ErrFailed matches %s", name)
		}
	}
}
