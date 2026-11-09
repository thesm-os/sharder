// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command operation-spec applies a new version of the spec of a namespace through the
// maintenance calls of the library. An application builds the command line of its operators on
// these calls.
//
// The program runs process-a and process-b in one OS process, over one in-memory store and one
// in-process network. Each process is an embedded.Process of the preset exclusive-worker, whose
// replicas have no work of their own. A maintenance.Client of the operator reads the status of
// the namespace, and submits the next version of the spec to the leader of the root. The run
// has three stages:
//
//   - The processes start, and the root creates the namespace from the initial spec.
//   - The operator applies a spec whose budget lets planned disruptions cover two nodes at once.
//     The base of the proposal is the version of the accepted spec.
//   - The root accepts the spec as version 2, and the controllers apply it.
//
// The program writes the versions before and after the change:
//
//	before: the namespace accepts version 1, and the cell applies version 1
//	apply: version 2 sets maxConcurrentNodes to 2
//	after: the namespace accepts version 2, and the cell applies version 2
//
// Run it from the examples module:
//
//	go run ./operation-spec
//
// The program logs the errors of the processes at the level error.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/crypto"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// namespace is the namespace of the processes.
const namespace shard.Namespace = "workers"

// The processes of the example.
const (
	// processA is the process that starts first.
	processA shard.NodeID = "process-a"
	// processB is the process that starts second.
	processB shard.NodeID = "process-b"
)

// The settings of the namespace and of its operator.
const (
	// port is the port of the address of each process.
	port = ":7000"
	// slots is the number of the slots of the spec of the namespace.
	slots = 4
	// concurrentNodes is the most nodes that admitted planned disruptions cover at once in the
	// new spec.
	concurrentNodes = 2
	// created is the version of the spec with which the root creates the namespace.
	created epoch.Epoch = 1
	// operator is the identity that the controllers admit to submit a proposal.
	operator shard.Identity = "operator"
	// lifetime is the time after which a proposal of the operator expires.
	lifetime = time.Minute
	// tick is the time between two reads of the status of the namespace.
	tick = 50 * time.Millisecond
)

// The output and the failure of the program.
const (
	// before is the stage of the status before the change.
	before = "before"
	// after is the stage of the status after the change.
	after = "after"
	// versionFormat formats the versions at one stage: the stage, the version of the accepted
	// spec, and the version that the controller of the cell applies.
	versionFormat = "%s: the namespace accepts version %d, and the cell applies version %d\n"
	// applyFormat formats the change: the version that the root accepts and the new bound.
	applyFormat = "apply: version %d sets maxConcurrentNodes to %d\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "operation-spec: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// processes lists the processes of the example in the order of their starts.
var processes = []shard.NodeID{processA, processB}

// initial is the initial spec of the namespace, which each process passes to its root.
var initial = config.Spec{
	Namespace: namespace,
	Preset:    builtin.ExclusiveWorker,
	Partitioning: config.Selection{
		Name:   fixedslots.Name,
		Config: json.RawMessage(fmt.Sprintf(`{"slots":%d}`, slots)),
	},
}

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the lines of the status.
	out io.Writer
	// clock reads the time of the store, of the processes and of the client.
	clock clock.Clock
	// rand is the source of randomness of the processes and of the client.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
	// caller is the identity of the client of the operator. The controllers admit operator
	// alone.
	caller shard.Identity
}

// idle opens the tenures of a node without work. The example shows a change of the spec, so
// its replicas have no work of their own.
type idle struct{}

var _ replica.Opener = idle{}

// Open returns a result without a Run.
func (idle) Open(context.Context, *replica.OpenRequest) (replica.OpenResult, error) {
	return replica.OpenResult{}, nil
}

// system is the store, the storage and the network that the processes of the example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the fenced storage of the replicas.
	storage *fencedstore.Store
	// net connects the processes and the client.
	net *local.Network
}

// start starts the process name with the initial spec under ctx, and returns once the process
// serves. The network of s serves the process, and the controller of the process admits the
// proposals of operator.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as config.ErrUnknown for a
//     registry without the built-in strategies, or the cause of ctx when ctx ends before the
//     process serves.
func (s *system) start(ctx context.Context, name shard.NodeID) (*embedded.Process, error) {
	n := shard.Node{ID: name, Address: string(name) + port, Identity: shard.Identity(name)}
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: initial, Handler: replica.NewRunner(idle{}), Guard: s.storage}},
		Store:      s.st,
		Membership: s.st.Membership(),
		Transport:  s.net.Local(protocol.Metadata{Caller: n.Identity, Version: protocol.Current}),
		Clock:      s.env.clock,
		Rand:       s.env.rand,
		Reporter:   noop.New(),
		Registry:   s.env.registry,
		Logger:     s.env.logger,
		Operators:  map[shard.Namespace][]shard.Identity{namespace: {operator}},
		Node:       n,
		Volatile:   true,
	})
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the process %s: %w", name, err)
	}
	s.net.AddAgent(namespace, n.ID, p.AgentServer())
	s.net.AddController(n.Address, p.ControllerServer())
	if err := p.Start(ctx); err != nil {
		return nil, fmt.Errorf(errorPrefix+"the start of %s: %w", name, err)
	}
	return p, nil
}

// run runs the example in env over one in-memory store and one in-process network. It builds
// the client of the operator, starts the processes, and waits until the cell applies the
// initial spec. It then applies the next version of the spec, and waits until the cell applies
// it. run writes the versions before and after the change to env.out once the run has ended.
// ctx bounds the waits and the calls, and the processes stop when run returns.
//
// Error modes:
//   - an error that wraps maintenance.ErrConfig for an environment without a source of
//     randomness.
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the error of Client.Apply, such as an error of class errs.Denied for
//     a caller that the controllers do not admit.
//   - an error that wraps the error of await, such as the cause of ctx when ctx ends before
//     the cell applies the new spec.
//   - an error that wraps the error of the write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork()}
	c, err := maintenance.New(&maintenance.Config{
		Store:      st,
		Membership: st.Membership(),
		Transport:  sys.net.Local(protocol.Metadata{Caller: env.caller, Version: protocol.Current}),
		Clock:      env.clock,
		Rand:       env.rand,
		Lifetime:   lifetime,
	})
	if err != nil {
		return fmt.Errorf(errorPrefix+"the client: %w", err)
	}
	procs, finish := context.WithCancel(context.WithoutCancel(ctx))
	var started []*embedded.Process
	defer func() {
		finish()
		for _, p := range started {
			<-p.Done()
		}
	}()
	for _, name := range processes {
		p, serr := sys.start(procs, name)
		if serr != nil {
			return serr
		}
		started = append(started, p)
	}
	first, err := await(ctx, env.clock, c, created)
	next := initial
	bound := concurrentNodes
	next.Budget.MaxConcurrentNodes = &bound
	var applied maintenance.ApplyResult
	if err == nil {
		applied, err = c.Apply(ctx, &maintenance.ApplyRequest{Spec: next})
	}
	var last maintenance.StatusResult
	if err == nil {
		last, err = await(ctx, env.clock, c, applied.Version)
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"the change of the spec of %s: %w", namespace, err)
	}
	out := fmt.Sprintf(versionFormat, before, first.Version, first.Cells[0].Record.SpecVersion) +
		fmt.Sprintf(applyFormat, applied.Version, bound) +
		fmt.Sprintf(versionFormat, after, last.Version, last.Cells[0].Record.SpecVersion)
	if _, werr := io.WriteString(env.out, out); werr != nil {
		return fmt.Errorf(errorPrefix+"the output: %w", werr)
	}
	return nil
}

// await reads the status of the namespace through c on clk, one tick at a time, until the
// namespace accepts the spec of the given version and the controller of its one cell applies
// it, and returns that status.
//
// Error modes:
//   - an error that wraps the error of Client.Status.
//   - the cause of ctx when ctx ends first.
func await(ctx context.Context, clk clock.Clock, c *maintenance.Client, version epoch.Epoch) (
	maintenance.StatusResult, error,
) {
	for {
		res, err := c.Status(ctx, &maintenance.StatusRequest{Namespace: namespace})
		if err != nil {
			return maintenance.StatusResult{}, fmt.Errorf(errorPrefix+"the status of %s: %w", namespace, err)
		}
		if res.Version == version && len(res.Cells) == 1 && res.Cells[0].Record.SpecVersion == version {
			return res, nil
		}
		if clock.Wait(ctx, clk, tick) != nil {
			return maintenance.StatusResult{}, context.Cause(ctx)
		}
	}
}

// main runs the example until the cell applies the new spec or the process receives an
// interrupt. After an error, it writes the error to os.Stderr and exits with the status 1.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	reg := config.NewRegistry(builtin.Support())
	err := builtin.Register(reg)
	if err == nil {
		err = run(ctx, &environment{
			out:      os.Stdout,
			clock:    hlc.New(1, clock.DefaultMaxOffset),
			rand:     crypto.New(),
			registry: reg,
			logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			caller:   operator,
		})
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitFailure)
	}
}
