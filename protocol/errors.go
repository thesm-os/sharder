// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

import (
	"errors"
	"fmt"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package protocol.
var (
	// ErrUnsupported reports a call whose version, required feature, callback or operation
	// kind the receiver does not implement. The receiver returns it before any effect. It
	// matches errors.ErrUnsupported, and classifies as errs.Unsupported.
	ErrUnsupported = errs.WithClass(
		fmt.Errorf("protocol: unsupported by the receiver: %w", errors.ErrUnsupported), errs.Unsupported)

	// ErrUnreachable reports a call that the transport does not deliver to a server, such
	// as a call while the transport is not connected to the node or the controller. It
	// classifies as errs.Transient, because a later call can succeed.
	ErrUnreachable = errs.WithClass(errors.New("protocol: the transport did not deliver the call"), errs.Transient)

	// ErrUnknownCall reports a Call that is not one of the two callbacks. A codec returns an
	// error that wraps it for such a Call other than zero. ParseCall and Call.UnmarshalText
	// return one for text that is not the spelling of a callback. It classifies as
	// errs.Invalid.
	ErrUnknownCall = errs.WithClass(errors.New("protocol: unknown callback"), errs.Invalid)

	// ErrUnknownPath reports a Path that is not one of the three paths. A codec returns an
	// error that wraps it for such a Path other than zero. ParsePath and Path.UnmarshalText
	// return one for text that is not the spelling of a path. It classifies as errs.Invalid.
	ErrUnknownPath = errs.WithClass(errors.New("protocol: unknown path"), errs.Invalid)
)
