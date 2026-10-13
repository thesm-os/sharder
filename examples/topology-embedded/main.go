// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command topology-embedded runs two application processes of one namespace in the topology
// embedded, where every application process is also a controller process. The example shows
// the controllers of the topology: the root of the namespace and the controller of its cell.
// It uses the preset exclusive-worker, because every spec selects a preset, and its replicas
// have no work of their own.
//
// The program runs process-a and process-b in one OS process, over one in-memory store and one
// in-process network. Each process is an embedded.Process, which runs the controller of the
// process and the agent of its node. The run has three stages:
//
//   - process-a starts with a spec of 4 slots, and process-b with a spec of 8 slots. The root
//     of process-a leads, and creates the namespace from its spec. The root of process-b takes
//     the spec that the root scope stores. The root places the cell on process-a through an
//     add to the agent of the root of process-a, which starts the controller of the cell.
//   - process-a closes. Its replicas move to process-b, and then its controller stops.
//   - The root of process-b leads, and places the cell on process-b.
//
// The program writes one line per event:
//
//	root: leads on process-b at term 2
//
// Run it from the examples module:
//
//	go run ./topology-embedded
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
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/crypto"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// namespace is the namespace of the processes.
const namespace shard.Namespace = "tasks"

// The processes of the example.
const (
	// processA is the process that starts first and closes once its root leads.
	processA shard.NodeID = "process-a"
	// processB is the process that leads once process-a has closed.
	processB shard.NodeID = "process-b"
)

// port is the port of the address of each process.
const port = ":7000"

// The waits of the example.
const (
	// tick is the time between two reads of the scopes of the namespace.
	tick = 50 * time.Millisecond
	// drainFor bounds the move of the replicas of process-a.
	drainFor = time.Minute
)

// The output and the failure of the program.
const (
	// startFormat formats the start of a process: the process and the slots of its spec.
	startFormat = "%s: starts with the spec of %d slots\n"
	// specFormat formats the spec that the root scope stores: its version and its slots.
	specFormat = "spec: version %d with %d slots\n"
	// rootFormat formats the leader of the root: the process and the term of the leader.
	rootFormat = "root: leads on %s at term %d\n"
	// cellFormat formats the host of the cell: the process and the term of the controller of
	// the cell.
	cellFormat = "cell: hosted on %s at term %d\n"
	// closeFormat formats the close of a process.
	closeFormat = "%s: closes\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "topology-embedded: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// The slots of the initial specs of the processes.
const (
	// slotsA is the number of the slots of the spec of process-a.
	slotsA = 4
	// slotsB is the number of the slots of the spec of process-b.
	slotsB = 8
)

// starts lists the processes of the example in the order of their starts, each with the slots
// of its initial spec.
var starts = []struct {
	// name is the name of the process.
	name shard.NodeID
	// slots is the number of the slots of the initial spec of the process.
	slots int
}{
	{name: processA, slots: slotsA},
	{name: processB, slots: slotsB},
}

// errNoCell is the error of a namespace whose root scope does not list one cell.
var errNoCell = errors.New("main: the namespace does not have one cell")

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the line of each event.
	out io.Writer
	// clock reads the time of the store and of the processes.
	clock clock.Clock
	// rand is the source of randomness of the processes.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
}

// idle opens the tenures of a node without work. The example shows the controllers of the
// topology, so its replicas have no work of their own.
type idle struct{}

var _ replica.Opener = idle{}

// Open returns a result without a Run.
func (idle) Open(context.Context, *replica.OpenRequest) (replica.OpenResult, error) {
	return replica.OpenResult{}, nil
}

// view is what the scopes of the namespace show about its controllers.
type view struct {
	// leader is the record of the leader of the root.
	leader shard.Cell
	// cell is the record of the controller of the cell.
	cell shard.Cell
	// version is the version of the spec that the root scope stores.
	version epoch.Epoch
	// slots is the number of the slots of the spec that the root scope stores.
	slots int
	// moving is true while a transition of the cell is in flight in the root scope.
	moving bool
}

// system is the store, the storage and the network that the processes of the example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the fenced storage of the replicas.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
}

// start starts the process name with an initial spec of the given slots under ctx, and returns
// once the process serves. The network of s serves the process.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as config.ErrUnknown for a
//     registry without the built-in strategies, or the cause of ctx when ctx ends before the
//     process serves.
func (s *system) start(ctx context.Context, name shard.NodeID, slots int) (*embedded.Process, error) {
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

// run runs the example in env over one in-memory store and one in-process network. It starts
// process-a and process-b, and waits until the root leads on process-a and the cell has a
// controller. It then closes process-a, and waits until the root leads on process-b and the
// cell is hosted on process-b. run writes one line per event to env.out once the run has
// ended. ctx bounds the waits and the close of process-a, and the processes stop when run
// returns.
//
// Error modes:
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the error of await, such as the cause of ctx when ctx ends before
//     the controllers have moved.
//   - an error that wraps the error of Process.Close for process-a.
//   - an error that wraps the error of the write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork()}
	procs, finish := context.WithCancel(context.WithoutCancel(ctx))
	var started []*embedded.Process
	defer func() {
		finish()
		for _, p := range started {
			<-p.Done()
		}
	}()
	var lines []string
	for _, s := range starts {
		p, serr := sys.start(procs, s.name, s.slots)
		if serr != nil {
			return serr
		}
		started = append(started, p)
		lines = append(lines, fmt.Sprintf(startFormat, s.name, s.slots))
	}
	a := started[0]
	v, err := await(ctx, env.clock, st, func(v view) bool {
		return v.leader.Controller == string(processA)+port && v.cell.Controller != "" && !v.moving
	})
	if err == nil {
		lines = append(lines,
			fmt.Sprintf(specFormat, v.version, v.slots),
			fmt.Sprintf(rootFormat, strings.TrimSuffix(v.leader.Controller, port), v.leader.Epoch),
			fmt.Sprintf(cellFormat, strings.TrimSuffix(v.cell.Controller, port), v.cell.Epoch))
	}
	drain, cancel := context.WithTimeout(ctx, drainFor)
	err = errors.Join(err, a.Close(drain))
	cancel()
	if err == nil {
		lines = append(lines, fmt.Sprintf(closeFormat, processA))
		v, err = await(ctx, env.clock, st, func(v view) bool {
			return v.leader.Controller == string(processB)+port && v.cell.Controller == string(processB)+port &&
				!v.moving
		})
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"the controllers: %w", err)
	}
	lines = append(lines,
		fmt.Sprintf(rootFormat, strings.TrimSuffix(v.leader.Controller, port), v.leader.Epoch),
		fmt.Sprintf(cellFormat, strings.TrimSuffix(v.cell.Controller, port), v.cell.Epoch))
	if _, werr := io.WriteString(env.out, strings.Join(lines, "")); werr != nil {
		return fmt.Errorf(errorPrefix+"the output: %w", werr)
	}
	return nil
}

// await reads the view of the scopes of the namespace from st on clk, one tick at a time, until
// done reports true for it, and returns that view.
//
// Error modes:
//   - the error of observe.
//   - the cause of ctx when ctx ends first.
func await(ctx context.Context, clk clock.Clock, st store.Reader, done func(v view) bool) (view, error) {
	for {
		v, err := observe(ctx, st)
		if err != nil {
			return view{}, err
		}
		if done(v) {
			return v, nil
		}
		if clock.Wait(ctx, clk, tick) != nil {
			return view{}, context.Cause(ctx)
		}
	}
}

// observe reads the view of the namespace from st. The root scope stores the spec and the
// record of the leader, and lists the cell. The scope of the cell stores the record of its
// controller.
//
// Error modes:
//   - an error that wraps errNoCell for a root scope that does not list one cell.
//   - an error that wraps the error of a read of the root scope or of the scope of the cell.
//   - an error that wraps the error of the decoding of the spec that the root scope stores.
func observe(ctx context.Context, st store.Reader) (view, error) {
	root, cur, err := st.Read(ctx, shard.Scope{Namespace: namespace}, shard.NamespaceIncarnation{}, store.Query{},
		page.Page{})
	var cells []shard.Placement
	if err == nil {
		cells, err = placements(ctx, cur)
	}
	if err == nil && len(cells) != 1 {
		err = errNoCell
	}
	var spec config.Spec
	if err == nil {
		err = json.Unmarshal(root.Spec, &spec)
	}
	var slots fixedslots.Config
	if err == nil {
		err = json.Unmarshal(spec.Partitioning.Config, &slots)
	}
	if err != nil {
		return view{}, fmt.Errorf(errorPrefix+"the root scope of %s: %w", namespace, err)
	}
	cell := shard.CellID(ulid.Format(cells[0].Shard.ID()))
	state, cur, err := st.Read(ctx, shard.Scope{Namespace: namespace, Cell: cell}, root.Head.Incarnation,
		store.Query{}, page.Page{})
	if err == nil {
		_, err = placements(ctx, cur)
	}
	if err != nil {
		return view{}, fmt.Errorf(errorPrefix+"the scope of cell %s: %w", cell, err)
	}
	return view{
		leader:  root.Cell,
		cell:    state.Cell,
		version: spec.Version,
		slots:   slots.Slots,
		moving:  len(root.Transitions) > 0,
	}, nil
}

// placements returns the placements of the page of cur, and closes cur.
//
// Error modes:
//   - an error that joins the error of the iteration of cur and the error of its Close.
func placements(ctx context.Context, cur page.Cursor[shard.Placement]) ([]shard.Placement, error) {
	var ps []shard.Placement
	var err error
	for p, perr := range cur.Seq(ctx) {
		if perr != nil {
			err = perr
			break
		}
		ps = append(ps, p)
	}
	return ps, errors.Join(err, cur.Close())
}

// main runs the example until the controllers have moved to process-b or the process receives
// an interrupt. After an error, it writes the error to os.Stderr and exits with the status 1.
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
