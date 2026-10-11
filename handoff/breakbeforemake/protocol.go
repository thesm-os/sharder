// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package breakbeforemake

import (
	"fmt"
	"time"

	"go.thesmos.sh/sharder/handoff"
	"go.thesmos.sh/sharder/shard"
)

// Name is the registered name of the protocol break-before-make.
const Name shard.StrategyName = "break-before-make"

// version is the version of the scripts of the protocol, which Version returns.
const version uint32 = 1

// Protocol is the handoff protocol break-before-make. [New] returns a Protocol.
//
// The script of a transfer has three steps:
//
//  1. A Drop on From at the epoch of the shard before the move. The script has the
//     step only when the transfer has a From.
//  2. A Commit of To as the writer at the new epoch.
//  3. An Add on To at the new epoch.
//
// The abort paths of the first two steps are empty. The abort path of the Add has three
// steps:
//
//  1. A Commit of Fallback as the writer at the abort epoch.
//  2. An Add on Fallback at the abort epoch.
//  3. A Drop on To at the new epoch.
//
// When the transfer has no Fallback, the abort path is one Commit without a writer. The
// abort path does not wait for To to stop. Fallback acquires the fence before it serves,
// and every later write of the tenure on To then fails.
//
// # Concurrency
//
// A Protocol does not change after New returns it, and it is safe for concurrent use.
//
// # Allocation contract
//
// Script does not allocate for a transfer with a To. Its scripts share the slices that
// New built.
type Protocol struct {
	// steps lists the steps of the script of a transfer with a From. The steps of a
	// transfer without a From are steps[1:].
	steps []handoff.Step
	// abort lists one abort path per element of steps, for a transfer with a Fallback.
	abort [][]handoff.Step
	// abortWithoutWriter lists one abort path per element of steps, for a transfer
	// without a Fallback.
	abortWithoutWriter [][]handoff.Step
}

var _ handoff.Protocol = (*Protocol)(nil)

// New returns the protocol break-before-make with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func New(cfg Config) (*Protocol, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	add, drop := time.Duration(cfg.AddTimeout), time.Duration(cfg.DropTimeout)
	toFallback := []handoff.Step{
		{Action: handoff.Commit, Side: handoff.Fallback},
		{Action: handoff.Add, Side: handoff.Fallback, Timeout: add},
		{Action: handoff.Drop, Side: handoff.To, Timeout: drop},
	}
	withoutWriter := []handoff.Step{{Action: handoff.Commit}}
	return &Protocol{
		steps: []handoff.Step{
			{Action: handoff.Drop, Side: handoff.From, Timeout: drop},
			{Action: handoff.Commit, Side: handoff.To},
			{Action: handoff.Add, Side: handoff.To, Timeout: add},
		},
		abort:              [][]handoff.Step{nil, nil, toFallback},
		abortWithoutWriter: [][]handoff.Step{nil, nil, withoutWriter},
	}, nil
}

// Name returns Name.
func (*Protocol) Name() shard.StrategyName {
	return Name
}

// Version returns 1. The version changes when the actions or the sides of a script
// change. A change of a timeout does not change it.
func (*Protocol) Version() uint32 {
	return version
}

// Script returns the script of t. The description of Protocol lists its steps. From and
// To can be the same node, because the core can move a replica from one incarnation of
// the process of a node to the next. Every caller shares the script, so a caller does
// not modify it.
//
// Error modes:
//   - an error that wraps handoff.ErrTransfer when t has no To.
//
// # Allocation contract
//
// Zero alloc for a transfer with a To.
func (p *Protocol) Script(t handoff.Transfer) (handoff.Script, error) {
	if t.To == "" {
		return handoff.Script{}, fmt.Errorf("%w: %s needs a node on the side %v", handoff.ErrTransfer, Name, handoff.To)
	}
	s := handoff.Script{Steps: p.steps, Abort: p.abort}
	if t.Fallback == "" {
		s.Abort = p.abortWithoutWriter
	}
	if t.From == "" {
		s.Steps, s.Abort = s.Steps[1:], s.Abort[1:]
	}
	return s, nil
}
