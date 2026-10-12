// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package budget

//go:generate go tool kanon -type=Decision

import "time"

// Decision is the result of Admit for one negotiable disruption.
//
// The zero Decision is a deferral without a reason and without a time to ask again.
//
// # Encoding
//
// kanon generates the codec of Decision. Allowed is field 1, Reason is field 2, and
// RetryAfter is field 3. encoding/json encodes a Decision as an object with the keys
// reason, retryAfter and allowed, and RetryAfter as a count of nanoseconds. The receipt of
// a drain proposal contains that form.
//
// # Concurrency
//
// A copy of a Decision is independent of the original.
type Decision struct {
	// Reason is the cause of a deferral, for the operator. It is empty in a Decision
	// that allows the disruption.
	Reason string `json:"reason"`
	// RetryAfter is the time that a caller waits after a deferral before it calls
	// Admit again.
	RetryAfter time.Duration `json:"retryAfter"`
	// Allowed is true when the disruption may start now.
	Allowed bool `json:"allowed"`
}
