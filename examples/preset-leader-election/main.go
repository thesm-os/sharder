// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command preset-leader-election runs one active instance of a periodic task under the preset
// leader-election. The partitioner singleton gives the namespace one shard, so the node of
// that shard is the leader. The leader runs the task, and writes each run through the fence of
// its tenure.
//
// The program runs two application processes, instance-a and instance-b, in one OS process,
// over one in-memory store and one in-process network. Each process is an embedded.Process,
// which runs the controller of the process and the agent of its node. The scheduler of a
// process opens the tenure of the shard for a replica.Runner, which runs the task until the
// drop of the tenure. The run has three stages:
//
//   - instance-a starts alone, and the controller places the shard on it. instance-a leads.
//   - Once the task has run, instance-b starts and waits as a standby.
//   - Once the task has run 4 times, instance-a closes. The shard moves to instance-b, which
//     leads and continues the runs where instance-a stopped.
//
// The program then writes one line per run with the node that ran it:
//
//	run-5: instance-b
//
// Run it from the examples module:
//
//	go run ./preset-leader-election
//
// The program logs the errors of the processes at the level error.
package main

import (
	"context"
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
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// namespace is the namespace of the schedulers.
const namespace shard.Namespace = "reports"

// The processes of the example.
const (
	// instanceA is the process that starts first and leads until it closes.
	instanceA = "instance-a"
	// instanceB is the process that starts once the task has run, and leads once instance-a
	// has closed.
	instanceB = "instance-b"
)

// port is the port of the address of each process.
const port = ":7000"

// The task of the example.
const (
	// runs is the number of the runs of the task.
	runs = 8
	// interval is the time between two runs of the task.
	interval = 50 * time.Millisecond
)

// drainFor bounds the move of the shard of instance-a.
const drainFor = time.Minute

// The output and the failure of the program.
const (
	// runSeparator separates the number of a run from the node that ran it in the log of the
	// runs.
	runSeparator = ":"
	// lineFormat formats the line of one run: the number of the run and the node that ran it.
	lineFormat = "run-%s: %s\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "preset-leader-election: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// The formats of the entries of the storage of the schedulers.
const (
	// runFormat formats the entry of one run of the task in the log of the runs: the number of
	// the run and the node that ran it.
	runFormat = "%d" + runSeparator + "%s"
	// leaderFormat formats the entry of one tenure in the leaders: the node and the epoch of
	// the tenure.
	leaderFormat = "%s@%d"
)

// spec is the initial spec of the namespace. It selects the preset leader-election, whose
// partitioner singleton gives the namespace one shard.
var spec = config.Spec{Namespace: namespace, Preset: builtin.LeaderElection}

// key is a key of the storage of the schedulers.
type key string

// The keys of the storage of the schedulers.
const (
	// runsKey is the key of the log of the runs. The log lists the runs of the task, in order,
	// separated by spaces.
	runsKey key = "runs"
	// leadersKey is the key of the leaders. The leaders list the tenures of the one shard of
	// the namespace in order, separated by spaces.
	leadersKey key = "leaders"
)

// schedulerConfig is the configuration of a scheduler.
type schedulerConfig struct {
	// storage is the storage of the runs. The agent of the node acquires its fence for each
	// tenure.
	storage *fencedstore.Store
	// clock times the runs of the task.
	clock clock.Clock
	// node is the node of the scheduler.
	node shard.NodeID
	// runs is the number of the runs of the task. The task stops after the last run.
	runs int
	// interval is the time between two runs of the task.
	interval time.Duration
}

// scheduler opens the tenure of the one shard of the namespace for a replica.Runner, and runs
// a periodic task while its node has the tenure. The partitioner singleton gives the namespace
// one shard, so the node of that shard is the leader.
//
// The work of a tenure runs the task once per interval. It appends each run to the log of the
// runs in a transaction that checks the fence with the token of the tenure. Open reads the log
// from a state that contains the acquisition of the fence, so a new leader continues the runs
// where the old leader stopped. The fence refuses the writes of an earlier tenure from the
// acquisition on, so each run happens once.
//
// # Concurrency
//
// A scheduler is safe for concurrent use.
type scheduler struct {
	// replica is the replica of the storage that Open reads when the replica has applied the
	// acquisition of the tenure.
	replica *fencedstore.Replica
	// cfg is the configuration of the scheduler.
	cfg schedulerConfig
}

var _ replica.Opener = (*scheduler)(nil)

// newScheduler returns the scheduler of cfg, with a new replica of the storage of cfg.
func newScheduler(cfg *schedulerConfig) *scheduler {
	return &scheduler{replica: cfg.storage.NewReplica(), cfg: *cfg}
}

// Open appends the tenure of req to the leaders, in one transaction with the key of the add,
// so an add that the scheduler receives again does not append the tenure twice. It then reads
// the log of the runs through Store.Reader, and returns the runs that remain as the work of
// the tenure.
//
// Error modes:
//   - an error that wraps the error of the transaction, such as epoch.ErrFenced for a tenure
//     that a later tenure has fenced, and the cause of ctx.
func (s *scheduler) Open(ctx context.Context, req *replica.OpenRequest) (replica.OpenResult, error) {
	t := req.Token
	k := req.Request.Key()
	entry := fmt.Sprintf(leaderFormat, s.cfg.node, t.Epoch)
	writer := s.cfg.storage.Writer(t)
	err := s.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
		if tx.Recorded(&k) {
			return nil
		}
		if err := writer.Check(ctx, tx); err != nil {
			//nolint:wrapcheck // Open wraps the error of the transaction.
			return err
		}
		list, _ := tx.Get(string(leadersKey))
		tx.Put(string(leadersKey), strings.TrimSpace(list+" "+entry))
		tx.Record(&k)
		return nil
	})
	if err != nil {
		return replica.OpenResult{}, fmt.Errorf(errorPrefix+"the tenure at epoch %d: %w", t.Epoch, err)
	}
	ran := len(runsOf(s.cfg.storage.Reader(&t, s.replica)))
	return replica.OpenResult{Run: func(ctx context.Context) error { return s.run(ctx, writer, ran) }}, nil
}

// run runs the task through writer once per interval, after the ran runs of the earlier
// tenures, until the last run. Each run appends its number and the node to the log of the
// runs.
//
// Error modes:
//   - an error that wraps the error of clock.Wait once ctx ends.
//   - an error that wraps the error of the transaction of a run, such as epoch.ErrFenced once
//     a later tenure has acquired the fence.
func (s *scheduler) run(ctx context.Context, writer fence.Writer[*fencedstore.Tx], ran int) error {
	for next := ran + 1; next <= s.cfg.runs; next++ {
		if err := clock.Wait(ctx, s.cfg.clock, s.cfg.interval); err != nil {
			return fmt.Errorf(errorPrefix+"the wait for run %d: %w", next, err)
		}
		entry := fmt.Sprintf(runFormat, next, s.cfg.node)
		err := s.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
			if err := writer.Check(ctx, tx); err != nil {
				//nolint:wrapcheck // run wraps the error of the transaction.
				return err
			}
			list, _ := tx.Get(string(runsKey))
			tx.Put(string(runsKey), strings.TrimSpace(list+" "+entry))
			return nil
		})
		if err != nil {
			return fmt.Errorf(errorPrefix+"run %d: %w", next, err)
		}
	}
	return nil
}

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the line of each run.
	out io.Writer
	// clock reads the time of the store and of the processes, and times the runs.
	clock clock.Clock
	// rand is the source of randomness of the processes.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
}

// system is the store, the storage and the network that the processes of the example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the runs.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
}

// start starts the process name under ctx, and returns once the process serves. The scheduler
// of the process runs the task of its tenure through a replica.Runner, and the network of s
// serves the process.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as config.ErrUnknown for a
//     registry without the built-in strategies, or the cause of ctx when ctx ends before the
//     process serves.
func (s *system) start(ctx context.Context, name string) (*embedded.Process, error) {
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	sch := newScheduler(&schedulerConfig{
		storage:  s.storage,
		clock:    s.env.clock,
		node:     node.ID,
		runs:     runs,
		interval: interval,
	})
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: spec, Handler: replica.NewRunner(sch), Guard: s.storage}},
		Store:      s.st,
		Membership: s.st.Membership(),
		Transport:  s.net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
		Clock:      s.env.clock,
		Rand:       s.env.rand,
		Reporter:   noop.New(),
		Registry:   s.env.registry,
		Logger:     s.env.logger,
		Node:       node,
		Volatile:   true,
	})
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the process %s: %w", name, err)
	}
	s.net.AddAgent(namespace, node.ID, p.AgentServer())
	s.net.AddController(node.Address, p.ControllerServer())
	if err := p.Start(ctx); err != nil {
		return nil, fmt.Errorf(errorPrefix+"the start of %s: %w", name, err)
	}
	return p, nil
}

// run runs the example in env over one in-memory store and one in-process network. It starts
// instance-a, and starts instance-b once the task has run. Once the task has run half of its
// runs, it closes instance-a, whose shard moves to instance-b. Once the task has run every
// run, run stops instance-b. It then writes one line per run to env.out with the node that ran
// it.
//
// Error modes:
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the cause of ctx when ctx ends before every run.
//   - an error that wraps the error of Process.Close for instance-a.
//   - an error that wraps the error of a write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork()}
	procs, finish := context.WithCancel(ctx)
	defer finish()
	a, err := sys.start(procs, instanceA)
	if err != nil {
		return err
	}
	err = await(ctx, env.clock, func() bool { return len(runsOf(sys.storage)) > 0 })
	var b *embedded.Process
	if err == nil {
		b, err = sys.start(procs, instanceB)
	}
	if err == nil {
		defer func() {
			finish()
			<-b.Done()
		}()
		err = await(ctx, env.clock, func() bool { return len(runsOf(sys.storage)) >= runs/2 })
	}
	drain, cancel := context.WithTimeout(ctx, drainFor)
	err = errors.Join(err, a.Close(drain))
	cancel()
	if err == nil {
		err = await(ctx, env.clock, func() bool { return len(runsOf(sys.storage)) == runs })
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"%d of %d runs: %w", len(runsOf(sys.storage)), runs, err)
	}
	for _, entry := range runsOf(sys.storage) {
		number, node, _ := strings.Cut(entry, runSeparator)
		if _, werr := fmt.Fprintf(env.out, lineFormat, number, node); werr != nil {
			return fmt.Errorf(errorPrefix+"the line of run %s: %w", number, werr)
		}
	}
	return nil
}

// await waits on clk one interval at a time until done reports true. It returns the cause of
// ctx when ctx ends first.
func await(ctx context.Context, clk clock.Clock, done func() bool) error {
	for !done() {
		if clock.Wait(ctx, clk, interval) != nil {
			return context.Cause(ctx)
		}
	}
	return nil
}

// runsOf returns the log of the runs that r contains: the entry of each run of the task, in
// order. A task without a run has an empty log.
func runsOf(r fencedstore.Reader) []string {
	list, _ := r.Get(string(runsKey))
	return strings.Fields(list)
}

// main runs the example until the task has run every run or the process receives an
// interrupt. After an error, it writes the error to os.Stderr and exits with the status 1.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	reg := config.NewRegistry(builtin.Support())
	err := builtin.Register(reg)
	if err == nil {
		err = run(ctx, &environment{
			out:      os.Stdout,
			clock:    hlc.New(1),
			rand:     crypto.New(),
			registry: reg,
			logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		})
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitFailure)
	}
}
