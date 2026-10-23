// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package ownership

import (
	"errors"
	"fmt"
	"time"
)

// Config is the configuration of exclusive ownership.
//
// The zero Config is not valid, because its Lease is not.
type Config struct {
	// Lease is the lease of the nodes of the namespace.
	Lease Lease `json:"lease"`
	// Fenced declares that every write of the shard data of the application goes
	// through a fence. The support matrix requires it.
	Fenced bool `json:"fenced"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid. It
// compares Renew with the validity of the lease only when MaxDriftRate is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when the TTL is not above zero.
//   - an error that wraps ErrConfig when MaxDriftRate is not from 0 up to 1.
//   - an error that wraps ErrConfig when Renew is not above zero.
//   - an error that wraps ErrConfig when Renew is not below the validity of the lease.
//   - an error that wraps ErrConfig when LockDelay is below zero.
//   - an error that wraps ErrConfig when OnLoss is not valid.
//   - an error that wraps ErrConfig when OnLoss is ContinueFenced and Fenced is false.
func (c Config) Validate() error {
	l := c.Lease
	var problems []error
	if l.TTL <= 0 {
		problems = append(problems, fmt.Errorf("%w: the TTL %v is not above zero", ErrConfig, time.Duration(l.TTL)))
	}
	// A NaN fails both comparisons.
	drift := l.MaxDriftRate >= 0 && l.MaxDriftRate < 1
	if !drift {
		problems = append(problems,
			fmt.Errorf("%w: the drift rate %v is not from 0 up to 1", ErrConfig, l.MaxDriftRate))
	}
	if l.Renew <= 0 {
		problems = append(problems,
			fmt.Errorf("%w: the renewal interval %v is not above zero", ErrConfig, time.Duration(l.Renew)))
	}
	if validity := l.Validity(); drift && time.Duration(l.Renew) >= validity {
		problems = append(problems, fmt.Errorf("%w: the renewal interval %v is not below the validity %v",
			ErrConfig, time.Duration(l.Renew), validity))
	}
	if l.LockDelay < 0 {
		problems = append(problems,
			fmt.Errorf("%w: the lock delay %v is below zero", ErrConfig, time.Duration(l.LockDelay)))
	}
	// errors.Join discards the nil error of a valid action.
	problems = append(problems, l.OnLoss.valid())
	if l.OnLoss == ContinueFenced && !c.Fenced {
		problems = append(problems, fmt.Errorf("%w: %v without a fence", ErrConfig, l.OnLoss))
	}
	return errors.Join(problems...)
}
