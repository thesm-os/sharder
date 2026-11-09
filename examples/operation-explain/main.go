// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command operation-explain explains the placement of a shard through the maintenance calls of
// the library. An application builds the command line of its operators on these calls.
//
// The program runs process-a and process-b in one OS process, over one in-memory store and one
// in-process network. Each process is an embedded.Process of the preset exclusive-worker, whose
// replicas have no work of their own. A maintenance.Client of the operator reads the status of
// the namespace, and submits the explanation of a shard of process-a to the controller of its
// cell. The run has three stages:
//
//   - The processes start, and the controller of the cell places the four shards of the
//     namespace on them.
//   - The operator asks for the explanation of the first shard of process-a. The base of the
//     proposal is the generation of the cell.
//   - The controller runs a pass for the shard, and returns the constraints that its replicas
//     break and the cost of each tier of the goals of the placement. It commits nothing.
//
// A commit of the cell can follow the read of the status, and the controller then refuses the
// proposal with an error of class errs.Conflict. A transition of the shard leads to an error of
// class errs.Transient. The program then reads the status again and submits a new proposal.
//
// The program writes the replicas of each node before and after the explanation:
//
//	before: process-a hosts 3 of the 4 shards
//	explain: shard 01KA0B3WJ8RQQ3Y5ZQH4M6N7P8 has 0 violations and the costs [0.5]
//	after: process-a hosts 3 of the 4 shards
//
// Run it from the examples module:
//
//	go run ./operation-explain
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
	"slices"
	"strings"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id/ulid"
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
	// processA is the process of the explained shard.
	processA shard.NodeID = "process-a"
	// processB is the other process of the cell.
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
	// before is the stage of the status before the explanation.
	before = "before"
	// after is the stage of the status after the explanation.
	after = "after"
	// nodeFormat formats the replicas of one node at one stage: the stage, the node, the number
	// of its replicas and the number of the shards of the namespace.
	nodeFormat = "%s: %s hosts %d of the %d shards\n"
	// explainFormat formats the explanation: the ULID of the shard, the number of the violations
	// of the constraints and the cost of each tier of the goals.
	explainFormat = "explain: shard %s has %d violations and the costs %v\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "operation-explain: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// processes lists the processes of the example in the order of their starts.
var processes = []shard.NodeID{processA, processB}

// errNoShard is the error of a status in which process-a hosts no shard.
var errNoShard = errors.New("main: process-a hosts no shard")

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

// idle opens the tenures of a node without work. The example shows an explanation, so its
// replicas have no work of their own.
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
// the client of the operator, starts the processes, and explains the placement of a shard of
// process-a. It then reads the status again. run writes the lines of the status before and
// after the explanation to env.out once the run has ended. ctx bounds the waits and the calls,
// and the processes stop when run returns.
//
// Error modes:
//   - an error that wraps maintenance.ErrConfig for an environment without a source of
//     randomness.
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the error of explain, such as an error of class errs.Denied for a
//     caller that the controllers do not admit.
//   - an error that wraps the error of await, such as the cause of ctx when ctx ends before
//     the cell places every shard.
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
	start, sid, res, err := explain(ctx, env.clock, c)
	var end maintenance.StatusResult
	if err == nil {
		end, err = await(ctx, env.clock, c, placed)
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"the explanation of a shard of %s: %w", processA, err)
	}
	e := &res.Explanation
	lines := append(nodes(before, &start),
		fmt.Sprintf(explainFormat, ulid.Format(sid.ID()), len(e.Violations), e.Costs))
	lines = append(lines, nodes(after, &end)...)
	if _, werr := io.WriteString(env.out, strings.Join(lines, "")); werr != nil {
		return fmt.Errorf(errorPrefix+"the output: %w", werr)
	}
	return nil
}

// explain explains the placement of a shard of process-a, and returns the status before the
// explanation, the shard and the explanation. It waits until the cell places every shard, and
// submits the explanation of the first shard of process-a. A commit of the cell after the read
// of the status makes the controller refuse the proposal with an error of class errs.Conflict,
// and a transition of the shard with an error of class errs.Transient. explain then reads the
// status again and submits a new proposal.
//
// Error modes:
//   - the error of await.
//   - an error that wraps errNoShard for a status in which process-a hosts no shard.
//   - an error that wraps the error of Client.Explain of any other class, such as errs.Denied
//     for a caller that the controllers do not admit.
func explain(ctx context.Context, clk clock.Clock, c *maintenance.Client) (
	maintenance.StatusResult, shard.ID, maintenance.ExplainResult, error,
) {
	for {
		res, err := await(ctx, clk, c, placed)
		if err != nil {
			return maintenance.StatusResult{}, shard.ID{}, maintenance.ExplainResult{}, err
		}
		a := slices.IndexFunc(res.Cells[0].Nodes, func(n maintenance.NodeStatus) bool { return n.Node.ID == processA })
		if a < 0 || len(res.Cells[0].Nodes[a].Replicas) == 0 {
			return maintenance.StatusResult{}, shard.ID{}, maintenance.ExplainResult{}, errNoShard
		}
		sid := res.Cells[0].Nodes[a].Replicas[0].Shard
		e, err := c.Explain(ctx, &maintenance.ExplainRequest{Namespace: namespace, Shard: sid})
		if class := errs.Classify(err); class == errs.Transient || class == errs.Conflict {
			continue
		}
		if err != nil {
			return maintenance.StatusResult{}, shard.ID{}, maintenance.ExplainResult{},
				fmt.Errorf(errorPrefix+"the explanation of shard %v: %w", sid, err)
		}
		return res, sid, e, nil
	}
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
// places every shard and has no transition in flight.
func placed(res *maintenance.StatusResult) bool {
	if len(res.Cells) != 1 || len(res.Cells[0].Transitions) > 0 || len(res.Cells[0].Nodes) != len(processes) {
		return false
	}
	hosted := 0
	for i := range res.Cells[0].Nodes {
		hosted += len(res.Cells[0].Nodes[i].Replicas)
	}
	return hosted == slots
}

// nodes returns one line for each node of the cells of res at the stage stage, in the order of
// the nodes.
func nodes(stage string, res *maintenance.StatusResult) []string {
	var lines []string
	for i := range res.Cells {
		for j := range res.Cells[i].Nodes {
			n := &res.Cells[i].Nodes[j]
			lines = append(lines, fmt.Sprintf(nodeFormat, stage, n.Node.ID, len(n.Replicas), slots))
		}
	}
	return lines
}

// main runs the example until the controller has explained the placement of a shard or the
// process receives an interrupt. After an error, it writes the error to os.Stderr and exits
// with the status 1.
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
