// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller

import (
	"context"
	"errors"
	"fmt"

	"go.thesmos.sh/core/task"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/shard"
)

// The machines that a namespace runs for its whole run, as the errors of their drivers name
// them.
const (
	// rootMachine is the root of the namespace.
	rootMachine = "the root"
	// agentMachine is the agent of the root.
	agentMachine = "the agent of the root"
)

// namespace is what the process runs for one namespace: the root, the agent of the root,
// and the host of the cells that the root places on the process.
type namespace struct {
	// root runs the root machine of the namespace.
	root *driver.Driver
	// agent runs the agent of the root.
	agent *driver.Driver
	// host runs the controllers of the cells of the process.
	host *host
	// name is the namespace.
	name shard.Namespace
}

// start runs the drivers of the root and of the agent of the root as tasks of g. The host
// then starts the controller of each cell as a task of g, under ctx.
//
// Error modes:
//   - the errors of task.Group.Go once ctx has ended.
func (n *namespace) start(ctx context.Context, g *task.Group) error {
	n.host.ctx, n.host.group = ctx, g
	return errors.Join(
		g.Go(n.supervise(n.root, rootMachine)),
		g.Go(n.supervise(n.agent, agentMachine)),
	)
}

// supervise returns the task that runs the driver d of the machine what. The task returns
// the error that stops the run, which wraps the cause of ctx once ctx ends, and the error
// of a machine that has stopped itself. Every such error stops the run of the process.
func (n *namespace) supervise(d *driver.Driver, what string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return fmt.Errorf("controller: %s of %s: %w", what, n.name, d.Run(ctx))
	}
}

// leave closes the agent of the root, which ends its session in the root scope without a
// disruption. leave returns once the Leave of the session has returned. It returns nil for an
// agent without a session to end: an agent that does not serve, and an agent whose run has
// stopped.
//
// Error modes:
//   - an error that wraps the error of the Leave, and an error that wraps the cause of ctx
//     when ctx ends before the Leave returns.
func (n *namespace) leave(ctx context.Context) error {
	_, err := n.agent.Call[struct{}](ctx, func(tok machine.ReplyToken) machine.Event {
		return machine.Close{Reply: tok}
	})
	if err == nil || errors.Is(err, driver.ErrStopped) || errors.Is(err, agent.ErrNotServing) {
		return nil
	}
	return fmt.Errorf("controller: the close of %s of %s: %w", agentMachine, n.name, err)
}

// newNamespace returns what the process runs for the namespace of the spec s.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of root.New for a spec that the root
//     refuses, such as a spec that does not resolve.
func (c *Controller) newNamespace(s *config.Spec) (*namespace, error) {
	ns := s.Namespace
	operators := c.cfg.Operators[ns]
	rm, err := root.New(&root.Config{
		Clock:           c.cfg.Clock,
		Rand:            c.cfg.Rand,
		Reporter:        c.cfg.Reporter,
		Provided:        c.provided,
		Logger:          c.cfg.Logger,
		Fits:            fits(c.cfg.Store, shard.Scope{Namespace: ns}),
		Registry:        c.cfg.Registry,
		Operators:       operators,
		Node:            c.cfg.Node,
		Spec:            *s,
		Parallelism:     c.parallelism,
		Unauthenticated: c.cfg.Unauthenticated,
	})
	h := &host{
		store:   c.cfg.Store,
		cells:   make(map[shard.CellID]*hosted),
		drivers: c.drivers,
		config: cell.Config{
			Clock:           c.cfg.Clock,
			Rand:            c.cfg.Rand,
			Reporter:        c.cfg.Reporter,
			Provided:        c.provided,
			Logger:          c.cfg.Logger,
			Registry:        c.cfg.Registry,
			Scope:           shard.Scope{Namespace: ns},
			Operators:       operators,
			Node:            c.cfg.Node,
			Parallelism:     c.parallelism,
			Unauthenticated: c.cfg.Unauthenticated,
		},
	}
	var am *agent.Machine
	if err == nil {
		am, err = agent.New(&agent.Config{
			Clock:           c.cfg.Clock,
			Rand:            c.cfg.Rand,
			Reporter:        c.cfg.Reporter,
			Provided:        c.provided,
			Logger:          c.cfg.Logger,
			Registry:        c.cfg.Registry,
			Namespace:       ns,
			Node:            c.node,
			Unauthenticated: c.cfg.Unauthenticated,
			Root:            true,
		})
	}
	n := &namespace{host: h, name: ns}
	if err == nil {
		dc := c.drivers
		dc.Machine = rm
		n.root, err = driver.New(&dc)
	}
	if err == nil {
		dc := c.drivers
		dc.Machine, dc.Handler = am, h
		n.agent, err = driver.New(&dc)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: the namespace %s: %w", ErrConfig, ns, err)
	}
	return n, nil
}
