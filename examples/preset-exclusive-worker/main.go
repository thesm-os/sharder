// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command preset-exclusive-worker runs the workers of a namespace under the preset
// exclusive-worker. Each worker runs the jobs of the tenants of its shards, and writes each job
// through the fence of the tenure of its shard.
//
// The program runs two application processes, worker-a and worker-b, in one OS process, over
// one in-memory store and one in-process network. Each process is an embedded.Process, which
// runs the controller of the process and the agent of its node. The worker of a process opens
// each tenure of its node for a replica.Runner, which runs the jobs of the tenure until the
// drop of the tenure. The run has three stages:
//
//   - worker-a starts alone, and the controller places the 8 shards of the namespace jobs on it.
//   - Once worker-a has run the first job of every tenant, worker-b starts. The controller moves
//     half of the shards to worker-b, which continues the jobs of their tenants.
//   - Once half of the jobs have run, worker-b closes. Its shards move back to worker-a, which
//     runs the remaining jobs.
//
// The program then writes one line per tenant with the history of the tenant:
//
//	tenant-06: 1:worker-a 2:worker-a 3:worker-b 4:worker-b 5:worker-b 6:worker-a 7:worker-a 8:worker-a
//
// Run it from the examples module:
//
//	go run ./preset-exclusive-worker
//
// The program logs the errors of the processes at the level error. The processes log at the
// level warn the errors that they retry, such as a report that arrives at the controller of the
// cell before the controller has the session of the node.
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
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/crypto"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// The namespace of the example.
const (
	// namespace is the namespace of the workers.
	namespace shard.Namespace = "jobs"
	// slotsConfig is the configuration of fixed-slots in the spec of the namespace: 8 shards.
	slotsConfig = `{"slots":8}`
	// churnConfig lets one pass of the planner move half of the shards of the cell and 4 shards
	// out of one node. The preset lets a pass move 9% of the shards of a cell. For the 8 shards
	// of the example, that is one shard per pass.
	churnConfig = `{"churn":{"maxShardFraction":0.5,"perNode":4}}`
)

// The processes of the example.
const (
	// workerA is the process that starts first and runs until every job has run.
	workerA = "worker-a"
	// workerB is the process that starts once every tenant has run a job, and closes once half
	// of the jobs have run.
	workerB = "worker-b"
)

// port is the port of the address of each process.
const port = ":7000"

// The jobs of the example.
const (
	// jobCount is the number of the jobs of each tenant.
	jobCount = 8
	// tick is the time that one job takes.
	tick = 50 * time.Millisecond
)

// drainFor bounds the move of the shards of worker-b.
const drainFor = time.Minute

// The failure of the program.
const (
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "preset-exclusive-worker: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// The formats of the entries of the storage of the workers.
const (
	// jobFormat formats the entry of one job in the history of its tenant: the number of the
	// job and the node that ran it.
	jobFormat = "%d:%s"
	// tenureFormat formats the entry of one tenure in the tenures of its shard: the node and
	// the epoch of the tenure.
	tenureFormat = "%s@%d"
)

// tenants lists the tenants of the example. The name of a tenant is its key.
var tenants = []string{
	"tenant-01", "tenant-02", "tenant-03", "tenant-04", "tenant-05", "tenant-06", "tenant-07", "tenant-08",
	"tenant-09", "tenant-10", "tenant-11", "tenant-12", "tenant-13", "tenant-14", "tenant-15", "tenant-16",
}

// spec is the initial spec of the namespace. It selects the preset exclusive-worker with 8
// slots, and lets the planner move up to half of the shards in one pass.
var spec = config.Spec{
	Namespace:    namespace,
	Preset:       builtin.ExclusiveWorker,
	Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	Placement:    config.Selection{Config: json.RawMessage(churnConfig)},
}

// prefix is the prefix of a key of the storage of the workers.
type prefix string

// The prefixes of the keys of the storage of the workers.
const (
	// historyPrefix starts the key of the history of a tenant, which the name of the tenant
	// ends. The history lists the jobs of the tenant that have run, in order, separated by
	// spaces.
	historyPrefix prefix = "history/"
	// tenuresPrefix starts the key of the tenures of a shard, which the ULID of the shard ends.
	// The tenures list the tenures of the shard in order, separated by spaces.
	tenuresPrefix prefix = "tenures/"
)

// progress is the number of the jobs of a tenant that have run.
type progress struct {
	// tenant is the name of the tenant.
	tenant string
	// ran is the number of the jobs of the tenant that have run.
	ran int
}

// workerConfig is the configuration of a worker.
type workerConfig struct {
	// storage is the storage of the jobs. The agent of the node acquires its fence for each
	// tenure.
	storage *fencedstore.Store
	// partitioner maps the name of each tenant to its position.
	partitioner partition.Partitioner
	// clock times the jobs.
	clock clock.Clock
	// node is the node of the worker.
	node shard.NodeID
	// jobs is the number of the jobs of each tenant.
	jobs int
	// tick is the time that one job takes.
	tick time.Duration
}

// worker opens the tenures of the shards of its node for a replica.Runner, and runs the jobs
// of the tenants of each tenure. A tenant belongs to the shard whose range contains the
// position of its name.
//
// The work of a tenure keeps the number of the jobs of each tenant that have run in memory. It
// runs one job per tick, and appends each job to the history of its tenant in a transaction
// that checks the fence with the token of the tenure. Open loads the histories from a state
// that contains the acquisition of the fence, so the work continues every tenant where the
// earlier tenures stopped. The fence refuses the writes of an earlier tenure from the
// acquisition on, so each job runs once.
//
// # Concurrency
//
// A worker is safe for concurrent use.
type worker struct {
	// replica is the replica of the storage that Open reads when the replica has applied the
	// acquisition of the tenure.
	replica *fencedstore.Replica
	// cfg is the configuration of the worker.
	cfg workerConfig
}

var _ replica.Opener = (*worker)(nil)

// newWorker returns the worker of cfg, with a new replica of the storage of cfg.
func newWorker(cfg *workerConfig) *worker {
	return &worker{replica: cfg.storage.NewReplica(), cfg: *cfg}
}

// Open appends the tenure of req to the tenures of its shard, in one transaction with the key
// of the add, so an add that the worker receives again does not append the tenure twice. It
// then loads the history of each tenant of the shard through Store.Reader, and returns the
// jobs of the tenants as the work of the tenure.
//
// Error modes:
//   - an error that wraps the error of the transaction, such as epoch.ErrFenced for a tenure
//     that a later tenure has fenced, and the cause of ctx.
func (w *worker) Open(ctx context.Context, req *replica.OpenRequest) (replica.OpenResult, error) {
	r, t := req.Request, req.Token
	sid := ulid.Format(r.Shard.ID())
	key := r.Key()
	tenures := string(tenuresPrefix) + sid
	entry := fmt.Sprintf(tenureFormat, w.cfg.node, t.Epoch)
	writer := w.cfg.storage.Writer(t)
	err := w.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
		if tx.Recorded(&key) {
			return nil
		}
		if err := writer.Check(ctx, tx); err != nil {
			//nolint:wrapcheck // Open wraps the error of the transaction.
			return err
		}
		list, _ := tx.Get(tenures)
		tx.Put(tenures, strings.TrimSpace(list+" "+entry))
		tx.Record(&key)
		return nil
	})
	if err != nil {
		return replica.OpenResult{}, fmt.Errorf(errorPrefix+"the tenure of shard %s at epoch %d: %w", sid, t.Epoch, err)
	}
	src := w.cfg.storage.Reader(&t, w.replica)
	var pending []progress
	for _, name := range w.tenantsOf(r.Range) {
		pending = append(pending, progress{tenant: name, ran: len(history(src, name))})
	}
	return replica.OpenResult{Run: func(ctx context.Context) error { return w.run(ctx, writer, pending) }}, nil
}

// tenantsOf returns the names of the tenants whose positions r contains, in the order of
// tenants.
func (w *worker) tenantsOf(r shard.Range) []string {
	var names []string
	for _, name := range tenants {
		if r.Contains(w.cfg.partitioner.Position(nil, []byte(name))) {
			names = append(names, name)
		}
	}
	return names
}

// run runs the jobs of the tenants of work through writer, one job per tick, until every job
// of the tenants has run. Each job appends its number and the node to the history of its
// tenant.
//
// Error modes:
//   - an error that wraps the error of clock.Wait once ctx ends.
//   - an error that wraps the error of the transaction of a job, such as epoch.ErrFenced once
//     a later tenure has acquired the fence.
func (w *worker) run(ctx context.Context, writer fence.Writer[*fencedstore.Tx], work []progress) error {
	for pending := true; pending; {
		pending = false
		for i := range work {
			p := &work[i]
			if p.ran == w.cfg.jobs {
				continue
			}
			pending = true
			next := p.ran + 1
			if err := clock.Wait(ctx, w.cfg.clock, w.cfg.tick); err != nil {
				return fmt.Errorf(errorPrefix+"the wait for job %d of %s: %w", next, p.tenant, err)
			}
			key := string(historyPrefix) + p.tenant
			entry := fmt.Sprintf(jobFormat, next, w.cfg.node)
			err := w.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
				if err := writer.Check(ctx, tx); err != nil {
					//nolint:wrapcheck // run wraps the error of the transaction.
					return err
				}
				list, _ := tx.Get(key)
				tx.Put(key, strings.TrimSpace(list+" "+entry))
				return nil
			})
			if err != nil {
				return fmt.Errorf(errorPrefix+"job %d of %s: %w", next, p.tenant, err)
			}
			p.ran = next
		}
	}
	return nil
}

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the history of each tenant.
	out io.Writer
	// clock reads the time of the store and of the processes, and times the jobs.
	clock clock.Clock
	// rand is the source of randomness of the processes.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
}

// system is the store, the storage, the network and the partitioner that the processes of the
// example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the jobs.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
	// part maps the name of each tenant to its position.
	part partition.Partitioner
}

// start starts the process name under ctx, and returns once the process serves. The worker
// of the process runs the jobs of its tenures through a replica.Runner, and the network of s
// serves the process.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as the cause of ctx when ctx ends
//     before the process serves.
func (s *system) start(ctx context.Context, name string) (*embedded.Process, error) {
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	w := newWorker(&workerConfig{
		storage:     s.storage,
		partitioner: s.part,
		clock:       s.env.clock,
		node:        node.ID,
		jobs:        jobCount,
		tick:        tick,
	})
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: spec, Handler: replica.NewRunner(w), Guard: s.storage}},
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
// worker-a, and starts worker-b once every tenant has run a job. Once half of the jobs have
// run, it closes worker-b, whose shards move back to worker-a. Once every job has run, run
// stops worker-a. It then writes one line per tenant to env.out with the history of the
// tenant.
//
// Error modes:
//   - an error that wraps the error of partitioner, such as config.ErrUnknown for a registry
//     without the built-in strategies.
//   - an error that wraps the error of the start of a process, such as embedded.ErrConfig for
//     an environment without a logger.
//   - an error that wraps the cause of ctx when ctx ends before every job has run.
//   - an error that wraps the error of Process.Close for worker-b.
//   - an error that wraps the error of a write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	part, err := partitioner(env.registry, st)
	if err != nil {
		return err
	}
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork(), part: part}
	procs, finish := context.WithCancel(ctx)
	defer finish()
	a, err := sys.start(procs, workerA)
	if err != nil {
		return err
	}
	defer func() {
		finish()
		<-a.Done()
	}()
	total := len(tenants) * jobCount
	err = await(ctx, env.clock, func() bool { return begun(sys.storage) })
	var b *embedded.Process
	if err == nil {
		b, err = sys.start(procs, workerB)
	}
	if err == nil {
		err = await(ctx, env.clock, func() bool { return ran(sys.storage) >= total/2 })
		drain, cancel := context.WithTimeout(ctx, drainFor)
		err = errors.Join(err, b.Close(drain))
		cancel()
	}
	if err == nil {
		err = await(ctx, env.clock, func() bool { return ran(sys.storage) == total })
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"%d of %d jobs have run: %w", ran(sys.storage), total, err)
	}
	for _, name := range tenants {
		if _, werr := fmt.Fprintf(
			env.out,
			"%s: %s\n",
			name,
			strings.Join(history(sys.storage, name), " "),
		); werr != nil {
			return fmt.Errorf(errorPrefix+"the history of %s: %w", name, werr)
		}
	}
	return nil
}

// partitioner resolves the spec of the namespace with reg and the capabilities of st, and
// returns the partitioner of the spec, which maps the name of each tenant to its position.
//
// Error modes:
//   - an error that wraps the error of config.Resolve, such as config.ErrUnknown for a
//     registry without the built-in strategies.
//   - an error that wraps the error of config.Build.
func partitioner(reg *config.Registry, st *memory.Store) (partition.Partitioner, error) {
	s := spec
	res, err := config.Resolve(reg, &s, map[config.Axis][]config.Capability{
		config.Store:      st.Provides(),
		config.Membership: st.Membership().Provides(),
	})
	var part partition.Partitioner
	if err == nil {
		part, err = config.Build[partition.Partitioner](reg, res, config.Partitioning)
	}
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the partitioner of %s: %w", namespace, err)
	}
	return part, nil
}

// await waits on clk one tick at a time until done reports true. It returns the cause of ctx
// when ctx ends first.
func await(ctx context.Context, clk clock.Clock, done func() bool) error {
	for !done() {
		if clock.Wait(ctx, clk, tick) != nil {
			return context.Cause(ctx)
		}
	}
	return nil
}

// history returns the history of the tenant that r contains: the entry of each job of the
// tenant that has run, in order. A tenant without a job has an empty history.
func history(r fencedstore.Reader, tenant string) []string {
	list, _ := r.Get(string(historyPrefix) + tenant)
	return strings.Fields(list)
}

// begun reports whether the history of every tenant in storage has a job.
func begun(storage *fencedstore.Store) bool {
	for _, name := range tenants {
		if len(history(storage, name)) == 0 {
			return false
		}
	}
	return true
}

// ran returns the number of the jobs that the histories of the tenants in storage list.
func ran(storage *fencedstore.Store) int {
	n := 0
	for _, name := range tenants {
		n += len(history(storage, name))
	}
	return n
}

// main runs the example until every job has run or the process receives an interrupt. After an
// error, it writes the error to os.Stderr and exits with the status 1.
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
