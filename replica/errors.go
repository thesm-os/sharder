// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica

import (
	"errors"

	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/internal/machine/agent"
)

// The errors of an agent. The errors other than ErrConfig and ErrStarted are the errors of
// the machine of the agent, so errors.Is matches them as returned.
var (
	// ErrConfig is the error of New for a Config or a Handler that New refuses, and of
	// Agent.Close for a context without a deadline. It classifies as errs.Invalid.
	ErrConfig = errs.WithClass(errors.New("replica: invalid configuration"), errs.Invalid)
	// ErrStarted is the error of a second call of Agent.Start. An agent runs once. It
	// classifies as errs.Invalid.
	ErrStarted = errs.WithClass(errors.New("replica: the agent has started"), errs.Invalid)
	// ErrNotWriter is the error of Agent.Token for a shard without a tenure on the node. It
	// classifies as errs.Conflict.
	ErrNotWriter = agent.ErrNotWriter
	// ErrNotServing is the error of a call before the agent serves and of a call after its
	// run has ended. A callback that waits when the agent stops gets it too. It classifies as
	// errs.Transient.
	ErrNotServing = agent.ErrNotServing
	// ErrNoReplica is the error of Agent.ReportFault and Agent.Recovered for a shard without
	// a replica on the node. It classifies as errs.NotFound.
	ErrNoReplica = agent.ErrNoReplica
	// ErrNoFault is the error of Agent.Recovered for a replica without a transient fault. It
	// classifies as errs.Conflict.
	ErrNoFault = agent.ErrNoFault
)
