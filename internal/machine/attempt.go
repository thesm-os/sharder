// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"context"
	"errors"

	"go.thesmos.sh/core/errs"
)

// ClassifyAttempt classifies the error err of one attempt of a callback, a fence acquisition
// or a call of the handler. A runtime bounds each attempt by the Timeout of its effect. A
// retry runs under a deadline of its own, so an attempt that ended at its deadline can
// succeed on a retry. ClassifyAttempt gives an err that wraps context.DeadlineExceeded the
// class errs.Transient. That class takes precedence over any class that err has.
//
// ClassifyAttempt returns every other err unchanged. An err that wraps context.Canceled keeps
// its class, because the caller has ended the attempt. ClassifyAttempt returns nil for nil.
func ClassifyAttempt(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errs.WithClass(err, errs.Transient)
	}
	return err
}
