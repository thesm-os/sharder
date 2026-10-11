// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocol

//go:generate go tool kanon -type=CallError

import "go.thesmos.sh/core/errs"

// CallError is an error as a transport sends it in place of a result: its class under
// errs.Classify and its message. The transport of the caller returns it as the error of
// the call, so the caller classifies it as the server classified its own error.
//
// The zero CallError is an error of the class errs.Unspecified without a message.
//
// # Encoding
//
// kanon generates the codec of CallError. ErrorClass is field 1, and Message is field 2.
//
// # Concurrency
//
// A CallError is a comparable value, and a copy is independent of the original.
type CallError struct {
	// Message is the text of the error of the server.
	Message string
	// ErrorClass is the class of the error of the server under errs.Classify.
	ErrorClass errs.Class
}

// Error returns the message of e.
//
// # Allocation contract
//
// Zero alloc.
func (e *CallError) Error() string {
	return e.Message
}

// Class returns the class of e, so errs.Classify returns the class of the error of the
// server.
//
// # Allocation contract
//
// Zero alloc.
func (e *CallError) Class() errs.Class {
	return e.ErrorClass
}
