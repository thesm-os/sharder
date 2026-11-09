// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command operation-undrain undrains a node of a namespace through the maintenance calls of the
// library. An application builds the command line of its operators on these calls.
//
// The program runs process-a and process-b in one OS process, over one in-memory store and one
// in-process network. Each process is an embedded.Process of the preset exclusive-worker, whose
// replicas have no work of their own. A maintenance.Client of the operator reads the status of
// the namespace, and submits the drain and the undrain of process-a to the controller of its
// cell. The run has three stages:
//
//   - The processes start, and the controller of the cell places the four shards of the
//     namespace on them. The operator drains process-a, and the controller moves every replica
//     of process-a to process-b.
//   - The operator undrains process-a. The commit of the undrain starts a pass of the planner.
//   - The pass moves a replica of process-b to process-a, because the goal count of the preset
//     balances the replicas of the nodes.
//
// The program writes the replicas of each node before and after the undrain:
//
//	before: process-a hosts 0 of the 4 shards and drains
//	undrain: process-a
//	after: process-a hosts 1 of the 4 shards
//
// Run it from the examples module:
//
//	go run ./operation-undrain
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
	"strings"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
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
	// processA is the process that the operator drains and undrains.
	processA shard.NodeID = "process-a"
	// processB is the process that hosts the replicas of process-a while process-a drains.
	processB shard.NodeID = "process-b"
)

// The settings of the namespace and of its operator.
const (
	// port is the port of the address of each process.
	port = ":7000"
	// slots is the number of the slots of the spec of the namespace.
	slots = 4
	// operator is the identity that the controllers admit to submit a proposal.
	operator shard.Identity = "operator"
	// lifetime is the time after which a proposal of the operator expires.
	lifetime = time.Minute
	// tick is the time between two reads of the status of the namespace.
	tick = 50 * time.Millisecond
)

// The output and the failure of the program.
const (
	// before is the stage of the status before the undrain.
	before = "before"
	// after is the stage of the status after the undrain.
	after = "after"
	// nodeFormat formats the replicas of one node at one stage: the stage, the node, the number
	// of its replicas and the number of the shards of the namespace.
	nodeFormat = "%s: %s hosts %d of the %d shards"
	// drainSuffix ends the line of a node that drains.
	drainSuffix = " and drains"
	// undrainFormat formats the undrain: the node.
	undrainFormat = "undrain: %s\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "operation-undrain: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// processes lists the processes of the example in the order of their starts.
var processes = []shard.NodeID{processA, processB}

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

// idle opens the tenures of a node without work. The example shows an undrain, so its replicas
// have no work of their own.
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

// start starts the process name under ctx, and returns once the process serves. The network of
// s serves the process, and the controller of the process admits the proposals of operator.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as config.ErrUnknown for a
//     registry without the built-in strategies, or the cause of ctx when ctx ends before the
//     process serves.
func (s *system) start(ctx context.Context, name shard.NodeID) (*embedded.Process, error) {
	n := shard.Node{ID: name, Address: string(name) + port, Identity: shard.Identity(name)}
	spec := config.Spec{
		Namespace: namespace,
		Preset:    builtin.ExclusiveWorker,
		Partitioning: config.Selection{
			Name:   fixedslots.Name,
			Config: json.RawMessage(fmt.Sprintf(`{"slots":%d}`, slots)),
		},
	}
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: spec, Handler: replica.NewRunner(idle{}), Guard: s.storage}},
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
// the client of the operator, starts the processes, and waits until the cell places every
// shard. It drains process-a, and waits until process-a hosts no replica. It then undrains
// process-a, and waits until process-a hosts a replica again. run writes the lines of the status
// before and after the undrain to env.out once the run has ended. ctx bounds the waits and the
// calls, and the processes stop when run returns.
//
// Error modes:
//   - an error that wraps maintenance.ErrConfig for an environment without a source of
//     randomness.
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the error of Client.Drain, such as an error of class errs.Denied for
//     a caller that the controllers do not admit.
//   - an error that wraps the error of await or of Client.Undrain, such as the cause of ctx
//     when ctx ends before the undrain has applied.
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
	_, err = await(ctx, env.clock, c, placed)
	if err == nil {
		_, err = c.Drain(ctx, &maintenance.DrainRequest{Namespace: namespace, Node: processA})
	}
	var empty maintenance.StatusResult
	if err == nil {
		empty, err = await(ctx, env.clock, c, drained)
	}
	if err == nil {
		_, err = c.Undrain(ctx, &maintenance.UndrainRequest{Namespace: namespace, Node: processA})
	}
	var back maintenance.StatusResult
	if err == nil {
		back, err = await(ctx, env.clock, c, undrained)
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"the undrain of %s: %w", processA, err)
	}
	lines := append(nodes(before, &empty), fmt.Sprintf(undrainFormat, processA))
	lines = append(lines, nodes(after, &back)...)
	if _, werr := io.WriteString(env.out, strings.Join(lines, "")); werr != nil {
		return fmt.Errorf(errorPrefix+"the output: %w", werr)
	}
	return nil
}

// await reads the status of the namespace through c on clk, one tick at a time, until done
// reports true for it, and returns that status.
//
// Error modes:
//   - an error that wraps the error of Client.Status.
//   - the cause of ctx when ctx ends first.
func await(ctx context.Context, clk clock.Clock, c *maintenance.Client, done func(*maintenance.StatusResult) bool) (
	maintenance.StatusResult, error,
) {
	for {
		res, err := c.Status(ctx, &maintenance.StatusRequest{Namespace: namespace})
		if err != nil {
			return maintenance.StatusResult{}, fmt.Errorf(errorPrefix+"the status of %s: %w", namespace, err)
		}
		if done(&res) {
			return res, nil
		}
		if clock.Wait(ctx, clk, tick) != nil {
			return maintenance.StatusResult{}, context.Cause(ctx)
		}
	}
}

// placed reports whether the view of the cell of res lists both processes, and whether the cell
// is settled.
func placed(res *maintenance.StatusResult) bool {
	_, ok := nodeA(res)
	return ok && len(res.Cells[0].Nodes) == len(processes)
}

// drained reports whether process-a drains and hosts no replica in a settled cell of res.
func drained(res *maintenance.StatusResult) bool {
	n, ok := nodeA(res)
	return ok && n.Draining && len(n.Replicas) == 0
}

// undrained reports whether process-a does not drain and hosts a replica in a settled cell of
// res.
func undrained(res *maintenance.StatusResult) bool {
	n, ok := nodeA(res)
	return ok && !n.Draining && len(n.Replicas) > 0
}

// nodeA returns the status of process-a in the one cell of res, and reports whether the cell is
// settled and has process-a. A settled cell places every shard and has no transition in
// flight.
func nodeA(res *maintenance.StatusResult) (*maintenance.NodeStatus, bool) {
	if len(res.Cells) != 1 || len(res.Cells[0].Transitions) > 0 {
		return nil, false
	}
	var a *maintenance.NodeStatus
	hosted := 0
	for i := range res.Cells[0].Nodes {
		n := &res.Cells[0].Nodes[i]
		hosted += len(n.Replicas)
		if n.Node.ID == processA {
			a = n
		}
	}
	return a, a != nil && hosted == slots
}

// nodes returns one line for each node of the cells of res at the stage stage, in the order of
// the nodes.
func nodes(stage string, res *maintenance.StatusResult) []string {
	var lines []string
	for i := range res.Cells {
		for j := range res.Cells[i].Nodes {
			n := &res.Cells[i].Nodes[j]
			line := fmt.Sprintf(nodeFormat, stage, n.Node.ID, len(n.Replicas), slots)
			if n.Draining {
				line += drainSuffix
			}
			lines = append(lines, line+"\n")
		}
	}
	return lines
}

// main runs the example until process-a hosts a replica again or the process receives an
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
