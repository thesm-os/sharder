// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/handoff/breakbeforemake"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
)

// settings are the parts of the spec of the namespace that the agent applies. The agent
// reads the spec once, when it starts.
type settings struct {
	// lease is the lease of the session of the node.
	lease ownership.Lease
	// window is the time between two reports of a node whose replicas do not change, the
	// window over which the application averages the loads of its shards.
	window time.Duration
	// add is the timeout of an add, the timeout of break-before-make for its fence
	// acquisition and for AddShard.
	add time.Duration
	// drop is the timeout of a drop and of a cleanup.
	drop time.Duration
	// writer is the writer role of the replica state model.
	writer shard.Role
}

// resolve returns the settings of the spec raw, as the root stores it, resolved with the
// registry r and the capabilities provided.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of encoding/json for a raw that does not
//     decode.
//   - an error that wraps ErrConfig and the error of config.Resolve for a spec that does not
//     resolve.
//   - an error that wraps ErrConfig and the errors of config.Build for an ownership mode or
//     a replica state model that does not build.
//   - an error that wraps ErrConfig for a handoff protocol other than break-before-make.
func resolve(r *config.Registry, provided map[config.Axis][]config.Capability, raw []byte) (settings, error) {
	var spec config.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return settings{}, fmt.Errorf("%w: the spec does not decode: %w", ErrConfig, err)
	}
	res, err := config.Resolve(r, &spec, provided)
	if err != nil {
		return settings{}, fmt.Errorf("%w: the spec of namespace %s: %w", ErrConfig, spec.Namespace, err)
	}
	own, oerr := config.Build[ownership.Config](r, res, config.Ownership)
	model, merr := config.Build[*replication.Compiled](r, res, config.Replication)
	timeouts, ok := res.Configs[config.Handoff].(*breakbeforemake.Config)
	var herr error
	if !ok {
		herr = fmt.Errorf("the handoff protocol %s is not %s", res.Spec.Handoff.Name, breakbeforemake.Name)
	}
	if err := errors.Join(oerr, merr, herr); err != nil {
		return settings{}, fmt.Errorf("%w: version %d of the spec of namespace %s: %w",
			ErrConfig, res.Spec.Version, spec.Namespace, err)
	}
	s := settings{
		lease:  own.Lease,
		window: time.Duration(res.Spec.Load.Window),
		add:    time.Duration(timeouts.AddTimeout),
		drop:   time.Duration(timeouts.DropTimeout),
	}
	for _, role := range model.Model.Roles {
		if role.Writer {
			s.writer = role.Name
		}
	}
	return s, nil
}
