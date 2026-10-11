// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake

import (
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/sharder/shard"
)

// Config is the configuration of the protocol break-before-make.
//
// The zero Config is not valid, because its timeouts are zero.
//
// # Encoding
//
// encoding/json encodes a Config as an object with the keys addTimeout and dropTimeout.
// A timeout is a Go duration string, such as "30s".
type Config struct {
	// AddTimeout bounds one attempt of an add. The handler of an add loads the data of
	// the shard before it returns, and the engine cancels a load that takes longer.
	AddTimeout shard.Duration `json:"addTimeout"`
	// DropTimeout bounds one attempt of a drop.
	DropTimeout shard.Duration `json:"dropTimeout"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when AddTimeout is not above zero.
//   - an error that wraps ErrConfig when DropTimeout is not above zero.
func (c Config) Validate() error {
	var problems []error
	if c.AddTimeout <= 0 {
		problems = append(problems,
			fmt.Errorf("%w: the add timeout %v is not above zero", ErrConfig, time.Duration(c.AddTimeout)))
	}
	if c.DropTimeout <= 0 {
		problems = append(problems,
			fmt.Errorf("%w: the drop timeout %v is not above zero", ErrConfig, time.Duration(c.DropTimeout)))
	}
	return errors.Join(problems...)
}
