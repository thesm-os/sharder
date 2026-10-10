// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

import (
	"fmt"
	"time"
)

// Duration is a time.Duration that a configuration writes in JSON as a Go duration
// string, such as "30s". The text methods make encoding/json write and read the
// string, and refuse a JSON number, because a bare number has no unit.
//
// The zero Duration is no time, and its text is "0s".
//
// # Concurrency
//
// A Duration is a comparable value.
type Duration time.Duration

// MarshalText returns the Go duration string of d, such as "1m30s". It returns no
// error.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

// UnmarshalText sets d to the duration of the Go duration string text, such as "30s".
// It leaves d unchanged when it returns an error.
//
// Error modes:
//   - an error that wraps ErrDuration when text is not a Go duration string.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDuration, err)
	}
	*d = Duration(v)
	return nil
}
