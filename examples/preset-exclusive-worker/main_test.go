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
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/partition"
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
	mainEnv = "PRESET_EXCLUSIVE_WORKER_MAIN"
	runMain = "1"
)

// The separators of the output of the program.
const (
	// tenantSeparator separates the name of a tenant from its history.
	tenantSeparator = ": "
	// jobSeparator separates the number of a job from the node that ran it.
	jobSeparator = ":"
)

// The jobs of the specs of the worker. A tenant of a shard with 4 tenants runs its first job
// at the fourth tick, so every tenant still has a job left once pair starts worker-b. A tenant
// needs at least 32 ticks for its jobs, which is longer than the drain of a process. A drain
// ends at the commit that ends the last move off the process, because the controller admits
// the disruption of the drain at that commit.
const (
	// fixtureJobs is the number of the jobs of each tenant.
	fixtureJobs = 32
	// fixtureTick is the time that one job takes.
	fixtureTick = 100 * time.Millisecond
	// rounds bounds the ticks of a wait for the histories.
	rounds = 200
	// stopFor is the time during which a spec checks that the jobs of a tenure have stopped.
	stopFor = 10 * fixtureTick
)

// logged is the history of two jobs that a spec of history writes.
const logged = "1:worker-a 2:worker-b"

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

// proc is a process of a spec of the worker, with its worker and the runner that runs the
// work of the worker.
type proc struct {
	*embedded.Process
	// worker opens the tenures of the node of the process.
	worker *worker
	// runner runs the work of each tenure of the node.
	runner *replica.Runner
}

// fixture is the store, the storage, the network, the registry and the partitioner that the
// processes of a spec of the worker share, and the processes that the spec starts.
type fixture struct {
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the jobs.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
	// reg is the registry of the processes.
	reg *config.Registry
	// part maps the name of each tenant to its position.
	part partition.Partitioner
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

		t.Run("writes the history of each tenant to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			for _, name := range tenants {
				expect.Contains(t, stdout.String(), name+tenantSeparator, "the command writes the history of "+name)
			}
			expect.Contains(t, stdout.String(), jobSeparator+workerB, "worker-b runs the jobs of the shards that move")
		})

		t.Run("exits with the status 1 when the write of a history fails", func(t *testing.T) {
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
			expect.Contains(t, stderr.String(), "the history of "+tenants[0], "the command writes the error")
		})
	})
}

// TestCommand checks the worker of the example, and the run of the example in the process of
// the test.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("worker", func(t *testing.T) {
		t.Parallel()

		t.Run("tenantsOf", func(t *testing.T) {
			t.Parallel()

			t.Run("returns every tenant for the whole range", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					a := newFixture(t).start(t, workerA)
					assert.Equal(
						t,
						a.worker.tenantsOf(shard.Range{}),
						tenants,
						"the whole range must contain every tenant",
					)
				})
			})
		})

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("records the tenure of an add once", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.pair(t)
					ra, rb := f.record(workerA), f.record(workerB)
					f.drain(t, workerB)
					drops, adds := rb.callbacks(protocol.Drop), ra.callbacks(protocol.Add)
					assert.NotEmpty(t, drops, "worker-b must have dropped its shards")
					assert.NotEmpty(t, adds, "worker-a must have taken the shards of worker-b")
					before := make(map[shard.ID]string)
					for _, r := range adds {
						before[r.Shard] = f.tenures(r.Shard)
					}
					f.replay(t, append(drops, adds...))
					for sid, want := range before {
						expect.Equal(t, f.tenures(sid), want, "an add that arrives again must not record a tenure")
					}
					f.finish(t)
				})
			})

			t.Run("reads the histories from the storage when the replica has not applied the acquisition",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						a, _ := f.pair(t)
						a.worker.replica.Pause()
						reads := a.worker.replica.Reads()
						<-time.After(2 * fixtureTick)
						r := f.record(workerA)
						f.drain(t, workerB)
						moved := make([]string, 0, len(tenants))
						for _, add := range r.callbacks(protocol.Add) {
							moved = append(moved, a.worker.tenantsOf(add.Range)...)
						}
						assert.NotEmpty(t, moved, "worker-a must have taken a shard of worker-b with a tenant")
						assert.Equal(t, a.worker.replica.Reads(), reads,
							"the worker must not read a replica that lacks the acquisition")
						a.worker.replica.Resume()
						f.finish(t)
					})
				})

			t.Run("returns an error that wraps epoch.ErrFenced for a tenure that a later tenure has fenced",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						adds, _ := f.moved(t)
						a := f.procs[workerA]
						current := token(t, a, adds[0].Shard)
						later := current
						later.Epoch++
						assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
						again := adds[0].CloneKanon()
						again.Step++
						_, err := a.worker.Open(t.Context(), &replica.OpenRequest{Request: again, Token: current})
						assert.ErrorIs(t, err, epoch.ErrFenced, "the fence must refuse the tenure")
					})
				})

			t.Run("returns a work that runs every job of every tenant in order", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.start(t, workerA)
					f.finish(t)
					for name, entries := range f.histories() {
						for _, entry := range entries {
							expect.HasSuffix(
								t,
								entry,
								jobSeparator+workerA,
								"the one process must run each job of "+name,
							)
						}
					}
				})
			})

			t.Run("returns a work that stops once a later tenure acquires the fence", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					adds, fenced := f.moved(t)
					for _, add := range adds {
						later := token(t, f.procs[workerA], add.Shard)
						later.Epoch++
						assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
					}
					before := f.histories()
					<-time.After(stopFor)
					after := f.histories()
					for _, name := range fenced {
						expect.InRange(t, len(before[name]), 0, fixtureJobs-1,
							"a tenant of a fenced tenure must have a job left")
						expect.Equal(t, after[name], before[name], "the jobs of a fenced tenure must stop")
					}
				})
			})

			t.Run("returns a work that stops at the drop of the tenure", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					adds, dropped := f.moved(t)
					for _, add := range adds {
						_, err := f.procs[workerA].runner.DropShard(
							t.Context(),
							&replica.DropShardRequest{Request: add},
						)
						assert.NoError(t, err, "the runner drops the tenure")
					}
					before := f.histories()
					<-time.After(stopFor)
					after := f.histories()
					for _, name := range dropped {
						expect.InRange(t, len(before[name]), 0, fixtureJobs-1,
							"a tenant of a dropped tenure must have a job left")
						expect.Equal(t, after[name], before[name], "the jobs of a dropped tenure must stop")
					}
				})
			})
		})
	})

	t.Run("history", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the entry of each job of the tenant in order", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(historyPrefix)+tenants[0], logged)
				return nil
			}), "the history commits")
			assert.Equal(t, history(storage, tenants[0]), []string{"1:worker-a", "2:worker-b"},
				"history must return the entries of the history")
		})

		t.Run("returns no entry for a tenant without a history", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, history(fencedstore.New(), tenants[0]), "a tenant without a job must have no entry")
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes each job of each tenant once in order", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
				assert.Length(t, lines, len(tenants), "run writes one line per tenant")
				for i, line := range lines {
					name, entries, ok := strings.Cut(line, tenantSeparator)
					assert.True(t, ok, "the line of a tenant starts with its name")
					expect.Equal(t, name, tenants[i], "the lines are in the order of the tenants")
					jobs := strings.Fields(entries)
					assert.Length(t, jobs, jobCount, "every job of "+name+" runs once")
					for j, entry := range jobs {
						number, _, _ := strings.Cut(entry, jobSeparator)
						expect.Equal(t, number, strconv.Itoa(j+1), "the jobs of "+name+" run in order")
					}
				}
			})
		})

		t.Run("runs the first job of every tenant on worker-a", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				for line := range strings.Lines(out.String()) {
					_, entries, _ := strings.Cut(line, tenantSeparator)
					expect.HasPrefix(t, entries, "1"+jobSeparator+workerA, "worker-a runs the first job of each tenant")
				}
			})
		})

		t.Run("runs the jobs of the shards that move on worker-b", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.Contains(t, out.String(), jobSeparator+workerB, "worker-b must run a job")
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

		t.Run("returns the cause of its context when the context ends before every job has run",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithTimeoutCause(t.Context(), tick, errStop)
					defer cancel()
					assert.ErrorIs(
						t,
						run(ctx, newEnvironment(t, io.Discard)),
						errStop,
						"run must return the cause of its context",
					)
				})
			})

		t.Run("returns the error of the write of a history", func(t *testing.T) {
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
				ctx, cancel := context.WithTimeoutCause(t.Context(), tick, errStop)
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

// newFixture returns a fixture without processes, with the partitioner of the spec of the
// namespace. Its store closes when t ends.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	st := memory.New(hlc.New(1))
	t.Cleanup(func() { assert.NoError(t, st.Close(), "the store closes") })
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(t, builtin.Register(reg), "the built-ins register")
	part, err := partitioner(reg, st)
	assert.NoError(t, err, "the partitioner of the spec builds")
	return &fixture{
		st:      st,
		storage: fencedstore.New(),
		net:     local.NewNetwork(),
		reg:     reg,
		part:    part,
		procs:   make(map[string]*proc),
	}
}

// start starts the process name of f under the context of t, and waits until every goroutine
// of the bubble waits. The worker of the process runs fixtureJobs jobs per tenant, one per
// fixtureTick, through a replica.Runner. The process stops when t ends.
func (f *fixture) start(t *testing.T, name string) *proc {
	t.Helper()
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	w := newWorker(&workerConfig{
		storage:     f.storage,
		partitioner: f.part,
		clock:       hlc.New(1),
		node:        node.ID,
		jobs:        fixtureJobs,
		tick:        fixtureTick,
	})
	runner := replica.NewRunner(w)
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
	pr := &proc{Process: p, worker: w, runner: runner}
	f.procs[name] = pr
	synctest.Wait()
	return pr
}

// pair starts worker-a, waits until every tenant has run a job, and then starts worker-b. The
// moves of the first placement on worker-a have then ended, so the controller moves half of
// the shards to worker-b in one pass.
func (f *fixture) pair(t *testing.T) (a, b *proc) {
	t.Helper()
	a = f.start(t, workerA)
	f.await(t, func() bool { return begun(f.storage) }, "the first job of every tenant")
	return a, f.start(t, workerB)
}

// drain closes the process name of f, whose shards move to the other processes, and waits
// until every goroutine of the bubble waits.
func (f *fixture) drain(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), drainFor)
	defer cancel()
	assert.NoError(t, f.procs[name].Close(ctx), "the shards of the process move")
	synctest.Wait()
}

// record makes a recorder pass the callbacks of the process name to the process, and returns
// the recorder.
func (f *fixture) record(name string) *recorder {
	r := &recorder{AgentServer: f.procs[name].AgentServer()}
	f.net.AddAgent(namespace, shard.NodeID(name), r)
	return r
}

// moved starts both workers through pair. It then records the callbacks of worker-a while
// worker-b closes. It returns the adds of the shards of worker-b that worker-a has taken, and
// the names of their tenants. It checks that worker-a has taken a shard with a tenant.
func (f *fixture) moved(t *testing.T) ([]*protocol.Request, []string) {
	t.Helper()
	f.pair(t)
	r := f.record(workerA)
	f.drain(t, workerB)
	adds := r.callbacks(protocol.Add)
	names := make([]string, 0, len(tenants))
	for _, add := range adds {
		names = append(names, f.procs[workerA].worker.tenantsOf(add.Range)...)
	}
	assert.NotEmpty(t, names, "worker-a must have taken a shard of worker-b with a tenant")
	return adds, names
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
// worker of the node, and a drop to its runner.
func (f *fixture) replay(t *testing.T, calls []*protocol.Request) {
	t.Helper()
	for _, r := range calls {
		p := f.procs[string(r.Node)]
		if r.Call == protocol.Add {
			_, err := p.worker.Open(t.Context(), &replica.OpenRequest{Request: r, Token: token(t, p, r.Shard)})
			assert.NoError(t, err, "the worker opens the tenure again")
			continue
		}
		_, err := p.runner.DropShard(t.Context(), &replica.DropShardRequest{Request: r})
		assert.NoError(t, err, "the runner takes the drop again")
	}
}

// tenures returns the tenures of the shard sid.
func (f *fixture) tenures(sid shard.ID) string {
	list, _ := f.storage.Get(string(tenuresPrefix) + ulid.Format(sid.ID()))
	return list
}

// histories returns the history of each tenant.
func (f *fixture) histories() map[string][]string {
	hs := make(map[string][]string, len(tenants))
	for _, name := range tenants {
		hs[name] = history(f.storage, name)
	}
	return hs
}

// await waits one tick of the bubble at a time until done reports true, and fails t after
// rounds ticks.
func (*fixture) await(t *testing.T, done func() bool, what string) {
	t.Helper()
	for range rounds {
		if done() {
			return
		}
		<-time.After(fixtureTick)
	}
	t.Fatalf("%s did not happen within %d ticks", what, rounds)
}

// finish waits until every job of every tenant has run, and checks that each job of each
// tenant has run once, in order.
func (f *fixture) finish(t *testing.T) {
	t.Helper()
	f.await(t, func() bool { return ran(f.storage) == len(tenants)*fixtureJobs }, "the last job of every tenant")
	for name, entries := range f.histories() {
		assert.Length(t, entries, fixtureJobs, "every job of "+name+" must have run once")
		for i, entry := range entries {
			number, _, _ := strings.Cut(entry, jobSeparator)
			assert.Equal(t, number, strconv.Itoa(i+1), "the jobs of "+name+" must run in order")
		}
	}
}
