// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/rand/seeded"
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

// mainEnv makes the test binary run main in place of its tests when it has the value runMain.
// go test sets GOCOVERDIR for a test binary built with coverage. The process writes its
// coverage counters there when it exits, and the test binary merges them into its profile.
const (
	mainEnv = "PRESET_LEADER_ELECTION_MAIN"
	runMain = "1"
)

// The parts of the output of the program.
const (
	// runPrefix starts the line of a run.
	runPrefix = "run-"
	// nodeSeparator separates the name of a run from the node that ran it.
	nodeSeparator = ": "
)

// The runs of the specs of the scheduler. The task takes 60 intervals for its runs, which is
// longer than the drain of a process. A drain ends at the commit that ends the last move off
// the process, because the controller admits the disruption of the drain at that commit.
const (
	// fixtureRuns is the number of the runs of the task.
	fixtureRuns = 60
	// fixtureInterval is the time between two runs of the task.
	fixtureInterval = 100 * time.Millisecond
	// rounds bounds the intervals of a wait for the log.
	rounds = 200
	// stopFor is the time during which a spec checks that the runs of a tenure have stopped.
	stopFor = 10 * fixtureInterval
)

// logged is the log of two runs that a spec writes.
const logged = "1:instance-a 2:instance-b"

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errWrite is the error of a writer that refuses every write.
	errWrite = errors.New("test: the writer refuses the write")
)

// refusing is an io.Writer that refuses every write.
type refusing struct{}

// Write returns errWrite.
func (refusing) Write([]byte) (int, error) {
	return 0, errWrite
}

// recorder is the agent server of a process that keeps a copy of each callback that it
// passes to the process.
type recorder struct {
	protocol.AgentServer
	// calls lists a copy of each callback, in order.
	calls []*protocol.Request
	// mu guards calls.
	mu sync.Mutex
}

// Do keeps a copy of r, and passes r to the process.
func (r *recorder) Do(ctx context.Context, md protocol.Metadata, req *protocol.Request) error {
	r.mu.Lock()
	r.calls = append(r.calls, req.CloneKanon())
	r.mu.Unlock()
	return r.AgentServer.Do(ctx, md, req)
}

// callbacks returns the callbacks of the call c for the shards of a cell that r has passed, in
// order. It leaves out the callbacks of the root, which place cells on the process.
func (r *recorder) callbacks(c protocol.Call) []*protocol.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(r.calls), func(req *protocol.Request) bool {
		return req.Call != c || req.Scope.Cell == ""
	})
}

// proc is a process of a spec of the scheduler, with its scheduler and the runner that runs
// the work of the scheduler.
type proc struct {
	*embedded.Process
	// scheduler opens the tenures of the node of the process.
	scheduler *scheduler
	// runner runs the work of each tenure of the node.
	runner *replica.Runner
}

// fixture is the store, the storage, the network and the registry that the processes of a
// spec of the scheduler share, and the processes that the spec starts.
type fixture struct {
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the runs.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
	// reg is the registry of the processes.
	reg *config.Registry
	// procs maps the name of each process that the spec started to the process.
	procs map[string]*proc
}

// TestMain runs main in place of the tests when mainEnv has the value runMain, and the tests
// otherwise.
func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == runMain {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestCommandProcess runs the command as a process of the test binary, one process at a time.
// Every process writes the coverage meta-data file of the binary into the one GOCOVERDIR under
// a temporary name that Go 1.27 makes from the time in nanoseconds alone. Two processes that
// exit in the same nanosecond would write one file.
func TestCommandProcess(t *testing.T) {
	t.Run("main", func(t *testing.T) {
		self, err := os.Executable()
		assert.NoError(t, err, "the test binary has a path")

		t.Run("writes the line of each run to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			for n := 1; n <= runs; n++ {
				expect.Contains(t, stdout.String(), runPrefix+strconv.Itoa(n)+nodeSeparator,
					"the command writes the line of run "+strconv.Itoa(n))
			}
			expect.Contains(t, stdout.String(), nodeSeparator+instanceB, "instance-b leads once instance-a has closed")
		})

		t.Run("exits with the status 1 when the write of a line fails", func(t *testing.T) {
			readOnly, err := os.Open(os.DevNull)
			assert.NoError(t, err, "the null device opens")
			t.Cleanup(func() { assert.NoError(t, readOnly.Close(), "the null device closes") })
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = readOnly, &stderr
			_, exited := errors.AsType[*exec.ExitError](cmd.Run())
			assert.True(t, exited, "the command exits with an error")
			expect.Equal(t, cmd.ProcessState.ExitCode(), exitFailure, "the command exits with the status 1")
			expect.Contains(t, stderr.String(), "the line of run 1", "the command writes the error")
		})
	})
}

// TestCommand checks the scheduler of the example, and the run of the example in the process
// of the test.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("scheduler", func(t *testing.T) {
		t.Parallel()

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("records the tenure of an add once", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.pair(t)
					ra, rb := f.record(instanceA), f.record(instanceB)
					f.drain(t, instanceA)
					drops, adds := ra.callbacks(protocol.Drop), rb.callbacks(protocol.Add)
					assert.NotEmpty(t, drops, "instance-a must have dropped the shard")
					assert.NotEmpty(t, adds, "instance-b must have taken the shard")
					want := f.leaders()
					f.replay(t, append(drops, adds...))
					expect.Equal(t, f.leaders(), want, "an add that arrives again must not record a tenure")
					f.finish(t)
				})
			})

			t.Run("reads the runs from the storage when the replica has not applied the acquisition",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						_, b := f.pair(t)
						b.scheduler.replica.Pause()
						reads := b.scheduler.replica.Reads()
						<-time.After(2 * fixtureInterval)
						r := f.record(instanceB)
						f.drain(t, instanceA)
						assert.NotEmpty(t, r.callbacks(protocol.Add), "instance-b must have taken the shard")
						assert.Equal(t, b.scheduler.replica.Reads(), reads,
							"the scheduler must not read a replica that lacks the acquisition")
						b.scheduler.replica.Resume()
						f.finish(t)
					})
				})

			t.Run("returns an error that wraps epoch.ErrFenced for a tenure that a later tenure has fenced",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						adds := f.moved(t)
						b := f.procs[instanceB]
						current := token(t, b, adds[0].Shard)
						later := current
						later.Epoch++
						assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
						again := adds[0].CloneKanon()
						again.Step++
						_, err := b.scheduler.Open(t.Context(), &replica.OpenRequest{Request: again, Token: current})
						assert.ErrorIs(t, err, epoch.ErrFenced, "the fence must refuse the tenure")
					})
				})

			t.Run("returns a work that runs every run of the task in order", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.start(t, instanceA)
					f.finish(t)
					for _, entry := range runsOf(f.storage) {
						expect.HasSuffix(
							t,
							entry,
							runSeparator+instanceA,
							"the one process must run each run of the task",
						)
					}
				})
			})

			t.Run("returns a work that stops once a later tenure acquires the fence", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					adds := f.moved(t)
					later := token(t, f.procs[instanceB], adds[0].Shard)
					later.Epoch++
					assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
					before := runsOf(f.storage)
					<-time.After(stopFor)
					expect.InRange(t, len(before), 0, fixtureRuns-1, "the task of a fenced tenure must have a run left")
					expect.Equal(t, runsOf(f.storage), before, "the runs of a fenced tenure must stop")
				})
			})

			t.Run("returns a work that stops at the drop of the tenure", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					adds := f.moved(t)
					_, err := f.procs[instanceB].runner.DropShard(
						t.Context(),
						&replica.DropShardRequest{Request: adds[0]},
					)
					assert.NoError(t, err, "the runner drops the tenure")
					before := runsOf(f.storage)
					<-time.After(stopFor)
					expect.InRange(
						t,
						len(before),
						0,
						fixtureRuns-1,
						"the task of a dropped tenure must have a run left",
					)
					expect.Equal(t, runsOf(f.storage), before, "the runs of a dropped tenure must stop")
				})
			})
		})
	})

	t.Run("runsOf", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the entry of each run in order", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(runsKey), logged)
				return nil
			}), "the log commits")
			assert.Equal(t, runsOf(storage), []string{"1:instance-a", "2:instance-b"},
				"runsOf must return the entries of the log")
		})

		t.Run("returns no entry for a task without a run", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, runsOf(fencedstore.New()), "a task without a run must have no entry")
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes each run once in order", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
				assert.Length(t, lines, runs, "run writes one line per run")
				for i, line := range lines {
					name, _, ok := strings.Cut(line, nodeSeparator)
					assert.True(t, ok, "the line of a run starts with its name")
					expect.Equal(t, name, runPrefix+strconv.Itoa(i+1), "the lines are in the order of the runs")
				}
			})
		})

		t.Run("runs the first run on instance-a", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.HasPrefix(t, out.String(), runPrefix+"1"+nodeSeparator+instanceA, "instance-a leads first")
			})
		})

		t.Run("runs the last run on instance-b", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.HasSuffix(t, out.String(), nodeSeparator+instanceB+"\n",
					"instance-b leads once instance-a has closed")
			})
		})

		t.Run("returns an error that wraps config.ErrUnknown for a registry without the built-in strategies",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					env := newEnvironment(t, io.Discard)
					env.registry = config.NewRegistry(builtin.Support())
					assert.ErrorIs(
						t,
						run(t.Context(), env),
						config.ErrUnknown,
						"the spec must not resolve without the preset",
					)
				})
			})

		t.Run("returns an error that wraps embedded.ErrConfig for an environment without a logger",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					env := newEnvironment(t, io.Discard)
					env.logger = nil
					assert.ErrorIs(
						t,
						run(t.Context(), env),
						embedded.ErrConfig,
						"the process must refuse an environment without a logger",
					)
				})
			})

		t.Run("returns the cause of its context when the context ends before a process serves",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancelCause(t.Context())
					cancel(errStop)
					assert.ErrorIs(
						t,
						run(ctx, newEnvironment(t, io.Discard)),
						errStop,
						"run must return the cause of its context",
					)
				})
			})

		t.Run("returns the cause of its context when the context ends before every run",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithTimeoutCause(t.Context(), interval/2, errStop)
					defer cancel()
					assert.ErrorIs(
						t,
						run(ctx, newEnvironment(t, io.Discard)),
						errStop,
						"run must return the cause of its context",
					)
				})
			})

		t.Run("returns the error of the write of a line", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				assert.ErrorIs(t, run(t.Context(), newEnvironment(t, refusing{})), errWrite,
					"run must return the error of the writer")
			})
		})
	})

	t.Run("await", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the cause of its context when the context ends first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeoutCause(t.Context(), interval, errStop)
				defer cancel()
				assert.ErrorIs(t, await(ctx, hlc.New(1), func() bool { return false }), errStop,
					"await must return the cause of its context")
			})
		})
	})
}

// newEnvironment returns the environment of a run of the example that writes to out. Inside a
// bubble of testing/synctest, the clock reads the virtual time of the bubble.
func newEnvironment(t *testing.T, out io.Writer) *environment {
	t.Helper()
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(t, builtin.Register(reg), "the built-ins register")
	return &environment{
		out:      out,
		clock:    hlc.New(1),
		rand:     seeded.New(1),
		registry: reg,
		logger:   slog.New(slog.DiscardHandler),
	}
}

// newFixture returns a fixture without processes. Its store closes when t ends.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	st := memory.New(hlc.New(1))
	t.Cleanup(func() { assert.NoError(t, st.Close(), "the store closes") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(t, builtin.Register(reg), "the built-ins register")
	return &fixture{
		st:      st,
		storage: fencedstore.New(),
		net:     local.NewNetwork(),
		reg:     reg,
		procs:   make(map[string]*proc),
	}
}

// start starts the process name of f under the context of t, and waits until every goroutine
// of the bubble waits. The scheduler of the process runs fixtureRuns runs of the task, one per
// fixtureInterval, through a replica.Runner. The process stops when t ends.
func (f *fixture) start(t *testing.T, name string) *proc {
	t.Helper()
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	sch := newScheduler(&schedulerConfig{
		storage:  f.storage,
		clock:    hlc.New(1),
		node:     node.ID,
		runs:     fixtureRuns,
		interval: fixtureInterval,
	})
	runner := replica.NewRunner(sch)
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: spec, Handler: runner, Guard: f.storage}},
		Store:      f.st,
		Membership: f.st.Membership(),
		Transport:  f.net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
		Clock:      hlc.New(1),
		Rand:       seeded.New(1),
		Reporter:   noop.New(),
		Registry:   f.reg,
		Logger:     slog.New(slog.DiscardHandler),
		Node:       node,
		Volatile:   true,
	})
	assert.NoError(t, err, "New returns the process")
	f.net.AddAgent(namespace, node.ID, p.AgentServer())
	f.net.AddController(node.Address, p.ControllerServer())
	assert.NoError(t, p.Start(t.Context()), "Start starts the process")
	t.Cleanup(func() { <-p.Done() })
	pr := &proc{Process: p, scheduler: sch, runner: runner}
	f.procs[name] = pr
	synctest.Wait()
	return pr
}

// pair starts instance-a, waits until the task has run, and then starts instance-b. The one
// shard of the namespace then remains on instance-a, which leads.
func (f *fixture) pair(t *testing.T) (a, b *proc) {
	t.Helper()
	a = f.start(t, instanceA)
	f.await(t, func() bool { return len(runsOf(f.storage)) > 0 }, "the first run of the task")
	return a, f.start(t, instanceB)
}

// drain closes the process name of f, whose shard moves to the other process, and waits until
// every goroutine of the bubble waits.
func (f *fixture) drain(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), drainFor)
	defer cancel()
	assert.NoError(t, f.procs[name].Close(ctx), "the shard of the process moves")
	synctest.Wait()
}

// record makes a recorder pass the callbacks of the process name to the process, and returns
// the recorder.
func (f *fixture) record(name string) *recorder {
	r := &recorder{AgentServer: f.procs[name].AgentServer()}
	f.net.AddAgent(namespace, shard.NodeID(name), r)
	return r
}

// moved starts both instances through pair. It then records the callbacks of instance-b while
// instance-a closes, and returns the adds of the shard that instance-b has taken. It checks
// that instance-b has taken the shard.
func (f *fixture) moved(t *testing.T) []*protocol.Request {
	t.Helper()
	f.pair(t)
	r := f.record(instanceB)
	f.drain(t, instanceA)
	adds := r.callbacks(protocol.Add)
	assert.NotEmpty(t, adds, "instance-b must have taken the shard of instance-a")
	return adds
}

// token returns the fencing token of the tenure of the shard sid on the node of p.
func token(t *testing.T, p *proc, sid shard.ID) fence.Token {
	t.Helper()
	a, ok := p.Agent(namespace)
	assert.True(t, ok, "the process has the agent of the namespace")
	res, err := a.Token(t.Context(), &replica.TokenRequest{Shard: sid})
	assert.NoError(t, err, "the node has the tenure of the shard")
	return res.Token
}

// replay passes each callback of calls to the node of the callback again: an add to the
// scheduler of the node, and a drop to its runner.
func (f *fixture) replay(t *testing.T, calls []*protocol.Request) {
	t.Helper()
	for _, r := range calls {
		p := f.procs[string(r.Node)]
		if r.Call == protocol.Add {
			_, err := p.scheduler.Open(t.Context(), &replica.OpenRequest{Request: r, Token: token(t, p, r.Shard)})
			assert.NoError(t, err, "the scheduler opens the tenure again")
			continue
		}
		_, err := p.runner.DropShard(t.Context(), &replica.DropShardRequest{Request: r})
		assert.NoError(t, err, "the runner takes the drop again")
	}
}

// leaders returns the leaders that the storage of f lists.
func (f *fixture) leaders() string {
	list, _ := f.storage.Get(string(leadersKey))
	return list
}

// await waits one interval of the bubble at a time until done reports true, and fails t after
// rounds intervals.
func (*fixture) await(t *testing.T, done func() bool, what string) {
	t.Helper()
	for range rounds {
		if done() {
			return
		}
		<-time.After(fixtureInterval)
	}
	t.Fatalf("%s did not happen within %d intervals", what, rounds)
}

// finish waits until the task has run every run, and checks that each run has happened once,
// in order.
func (f *fixture) finish(t *testing.T) {
	t.Helper()
	f.await(t, func() bool { return len(runsOf(f.storage)) == fixtureRuns }, "the last run of the task")
	for i, entry := range runsOf(f.storage) {
		number, _, _ := strings.Cut(entry, runSeparator)
		assert.Equal(t, number, strconv.Itoa(i+1), "the runs of the task must be in order")
	}
}
