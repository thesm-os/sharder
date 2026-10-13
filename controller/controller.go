// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"runtime"
	"slices"
	"sync/atomic"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/task"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// Config is the configuration of a [Controller]. [New] refuses a Config without one of its
// required fields.
type Config struct {
	// Store keeps the scopes of each namespace. It is required. The capabilities of a store
	// that implements config.Provider decide which strategies a spec may select.
	Store store.Store
	// Membership keeps the sessions of the process in each root scope and the sessions of
	// the nodes of each cell. It is required. The capabilities of a membership that
	// implements config.Provider decide which strategies a spec may select.
	Membership membership.Membership
	// Agents sends the callbacks of each root to the controller processes, and the callbacks
	// of each hosted cell to its nodes. It is required.
	Agents protocol.AgentClient
	// Clock reads the time of each record, lease, timer and deadline of the process. It is
	// required.
	Clock clock.Clock
	// Rand is the source of the jitter of each backoff, of each new ID and of the seed of each
	// planning pass. Each machine of the process calls it from the goroutine of its driver,
	// so it is safe for concurrent use, as rand/seeded and rand/crypto are. It is required.
	Rand rand.Rand
	// Reporter reports the instruments of each root, agent and cell of the process. It is
	// required.
	Reporter telemetry.Reporter
	// Registry builds the strategies of each namespace from its spec. It is required.
	Registry *config.Registry
	// Logger logs the errors that do not stop a machine, such as a planning pass that fails.
	// It is required.
	Logger *slog.Logger
	// Operators maps each namespace to the identities that may submit a proposal, and
	// request or end any disruption. A namespace without an entry has no operator. New keeps
	// the map, so the caller does not modify it afterwards.
	Operators map[shard.Namespace][]shard.Identity
	// Node is the description of the controller process. The process joins the root of each
	// namespace with it and campaigns under it. The record of each scope that the process
	// acquires contains its address and its identity. It requires an ID and an address, and
	// it has no cell. In each root scope, the capacity "cells" of the node is CellCapacity.
	Node shard.Node
	// CellCapacity is the most cells that the root of a namespace places on the process.
	// Zero means one.
	CellCapacity int
	// Volatile makes New take a store that does not provide config.Durable, such as
	// store/memory in tests and simulations. A process on a volatile store loses its
	// transitions and its reservations when the store loses its state.
	Volatile bool
	// Unauthenticated makes the process skip the identity of each caller, for a transport
	// that authenticates no caller. The checks of epochs and incarnations still apply.
	Unauthenticated bool
}

// validate returns an error that wraps ErrConfig for the first field of c that New refuses.
func (c *Config) validate() error {
	if c.Store == nil {
		return fmt.Errorf("%w: no store", ErrConfig)
	}
	if c.Membership == nil {
		return fmt.Errorf("%w: no membership", ErrConfig)
	}
	if c.Agents == nil {
		return fmt.Errorf("%w: no agent client", ErrConfig)
	}
	if c.Clock == nil {
		return fmt.Errorf("%w: no clock", ErrConfig)
	}
	if c.Rand == nil {
		return fmt.Errorf("%w: no source of randomness", ErrConfig)
	}
	if c.Reporter == nil {
		return fmt.Errorf("%w: no reporter", ErrConfig)
	}
	if c.Registry == nil {
		return fmt.Errorf("%w: no registry", ErrConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: no logger", ErrConfig)
	}
	if c.Node.ID == "" {
		return fmt.Errorf("%w: no ID of the node", ErrConfig)
	}
	if c.Node.Address == "" {
		return fmt.Errorf("%w: no address of the node", ErrConfig)
	}
	if c.Node.Cell != "" {
		return fmt.Errorf("%w: the controller process %s has the cell %s", ErrConfig, c.Node.ID, c.Node.Cell)
	}
	if c.CellCapacity < 0 {
		return fmt.Errorf("%w: the cell capacity %d is negative", ErrConfig, c.CellCapacity)
	}
	return nil
}

// Controller is one controller process. It runs the root of each namespace of the process,
// and the controller of each cell that a root places on the process. It serves the callbacks
// of the roots as a protocol.AgentServer, and the reports, the proposals and the disruptions
// of each namespace as a protocol.ControllerServer.
//
// The zero Controller is not usable. Construct one with [New].
//
// # Concurrency
//
// A Controller is safe for concurrent use. Run runs once, and any number of goroutines
// call the methods of the servers.
type Controller struct {
	// served maps each namespace of the run to what the process runs for it. It points to a
	// nil map before Run.
	served atomic.Pointer[map[shard.Namespace]*namespace]
	// provided maps config.Store and config.Membership to the capabilities of their
	// adapters.
	provided map[config.Axis][]config.Capability
	// drivers is the configuration of the driver of each machine, without its machine and
	// its handler.
	drivers driver.Config
	// node is the description of the process in each root scope, with its cell capacity.
	node shard.Node
	// cfg is the configuration of the process.
	cfg Config
	// parallelism is the most evaluations that a planning pass runs at once.
	parallelism int
	// ran reports that Run has been called.
	ran atomic.Bool
}

// The servers that a Controller implements.
var (
	_ protocol.AgentServer      = (*Controller)(nil)
	_ protocol.ControllerServer = (*Controller)(nil)
)

// New returns the controller process of cfg before Run. The parallelism of each planning
// pass is runtime.GOMAXPROCS at the call.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Store, Membership,
//     Agents, Clock, Rand, Reporter, Registry, Logger, or the ID or the address of Node.
//   - an error that wraps ErrConfig for a Node with a cell, and for a negative CellCapacity.
//   - an error that wraps ErrConfig for a Store that does not provide config.Durable,
//     unless Volatile is set.
func New(cfg *Config) (*Controller, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	provided := map[config.Axis][]config.Capability{
		config.Store:      provides(cfg.Store),
		config.Membership: provides(cfg.Membership),
	}
	if !cfg.Volatile && !slices.Contains(provided[config.Store], config.Durable) {
		return nil, fmt.Errorf("%w: the store does not provide %v, and the controller is not volatile",
			ErrConfig, config.Durable)
	}
	node := cfg.Node
	node.Capacity = make(map[string]float64, len(cfg.Node.Capacity)+1)
	maps.Copy(node.Capacity, cfg.Node.Capacity)
	node.Capacity[root.Capacity] = float64(max(cfg.CellCapacity, 1))
	c := &Controller{
		provided: provided,
		drivers: driver.Config{
			Reader:     cfg.Store,
			Store:      cfg.Store,
			Membership: cfg.Membership,
			Agents:     cfg.Agents,
			Clock:      cfg.Clock,
			Logger:     cfg.Logger,
		},
		cfg:         *cfg,
		node:        node,
		parallelism: runtime.GOMAXPROCS(0),
	}
	c.served.Store(new(map[shard.Namespace]*namespace))
	return c, nil
}

// Run runs the namespace of each spec in specs until ctx ends. For each namespace, the
// process runs its root, which campaigns for the root scope, and the agent of the root. The
// root that leads creates the namespace from its spec when the root scope has no commit, and
// every root then takes the spec that the root scope stores. The agent of the root joins the
// root scope and runs the callbacks of the root, which start and stop the controller of each
// cell that the root places on the process. Each machine runs under a driver as a task of
// core's task.Run, so Run returns once every goroutine that it started has returned.
//
// The run stops when a root or an agent of a root stops itself. It stops when the
// controller of a cell stops itself after it served, unless a newer controller of the cell
// has fenced it. A controller of a cell whose acquisition fails, and a controller that the
// root drops, end without the run. Run always returns an error, because only an error
// stops the run.
//
// Error modes:
//   - ErrRun when Run has been called before.
//   - an error that wraps ErrConfig for a run without a spec, and for two specs of one
//     namespace.
//   - an error that wraps ErrConfig for a spec that the root refuses, such as a spec that
//     does not resolve with the capabilities of the store and the membership.
//   - an error that wraps the error of a root or an agent of a root that has stopped itself,
//     such as an agent of a namespace that has a new incarnation.
//   - an error that wraps the error of a controller of a cell that has stopped itself after
//     it served.
//   - an error that wraps the cause of ctx once ctx ends.
func (c *Controller) Run(ctx context.Context, specs ...config.Spec) error {
	if c.ran.Swap(true) {
		return ErrRun
	}
	served, err := c.namespaces(specs)
	if err != nil {
		return err
	}
	c.served.Store(&served)
	err = task.Run(ctx, math.MaxInt, func(ctx context.Context, g *task.Group) error {
		var started error
		for _, n := range served {
			started = errors.Join(started, n.start(ctx, g))
		}
		//dokimi:mutate-skip sbr-zero: the group records each refusal of Go as its own error, so task.Run returns the same error without started
		return started
	})
	return fmt.Errorf("controller: the run of %s stopped: %w", c.cfg.Node.ID, err)
}

// namespaces returns what the process runs for the namespace of each spec in specs.
//
// Error modes:
//   - an error that wraps ErrConfig for no spec, and for two specs of one namespace.
//   - the error of newNamespace.
func (c *Controller) namespaces(specs []config.Spec) (map[shard.Namespace]*namespace, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: no spec", ErrConfig)
	}
	served := make(map[shard.Namespace]*namespace, len(specs))
	for i := range specs {
		ns := specs[i].Namespace
		if _, ok := served[ns]; ok {
			return nil, fmt.Errorf("%w: two specs of the namespace %s", ErrConfig, ns)
		}
		n, err := c.newNamespace(&specs[i])
		if err != nil {
			return nil, err
		}
		served[ns] = n
	}
	return served, nil
}

// provides returns the capabilities of an adapter that implements config.Provider, and nil
// for any other adapter.
func provides(adapter any) []config.Capability {
	if p, ok := adapter.(config.Provider); ok {
		return p.Provides()
	}
	return nil
}

// fits returns the bound of a commit to the scope s of st, in the form of root.Config.Fits
// and cell.Config.Fits.
func fits(st store.Store, s shard.Scope) func(m *store.Mutation) error {
	return func(m *store.Mutation) error {
		if err := st.Fits(s, m); err != nil {
			return fmt.Errorf("controller: a commit to %v: %w", s, err)
		}
		return nil
	}
}
