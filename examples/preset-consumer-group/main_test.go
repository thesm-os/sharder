// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	"go.thesmos.sh/core/id"
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
	mainEnv = "PRESET_CONSUMER_GROUP_MAIN"
	runMain = "1"
)

// The separators of the output of the program.
const (
	// partitionSeparator separates the name of a partition from its log.
	partitionSeparator = ": "
	// messageSeparator separates the number of a message from the node that consumed it.
	messageSeparator = ":"
)

// The messages of the specs of the consumer. A partition takes at least 40 ticks to consume
// its messages, which is longer than the drain of a process. A drain ends at the commit that
// ends the last move off the process, because the controller admits the disruption of the
// drain at that commit.
const (
	// fixtureMessages is the number of the messages of each partition.
	fixtureMessages = 40
	// fixtureTick is the time that the consumption of one message takes.
	fixtureTick = 100 * time.Millisecond
	// rounds bounds the ticks of a wait for the logs.
	rounds = 200
	// stopFor is the time during which a spec checks that the consumption of a tenure has
	// stopped.
	stopFor = 10 * fixtureTick
)

// The partition and the entries that the specs of offsetOf and logOf write.
const (
	// partition is the partition whose offset and log a spec writes.
	partition = 2
	// logged is the log of two messages that a spec writes.
	logged = "1:consumer-a 2:consumer-b"
	// committed is the offset that a spec commits.
	committed = "2"
	// garbled is an offset that is not a number.
	garbled = "two"
)

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errWrite is the error of a writer that refuses every write.
	errWrite = errors.New("test: the writer refuses the write")
)

// sid is the shard of a tenure that a spec opens without a process.
var sid = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x01}}

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

// proc is a process of a spec of the consumer, with its consumer and the runner that runs the
// work of the consumer.
type proc struct {
	*embedded.Process
	// consumer opens the tenures of the node of the process.
	consumer *consumer
	// runner runs the work of each tenure of the node.
	runner *replica.Runner
}

// fixture is the store, the storage, the network and the registry that the processes of a
// spec of the consumer share, and the processes that the spec starts.
type fixture struct {
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the offsets and the logs.
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

		t.Run("writes the log of each partition to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			for p := range partitions {
				name := fmt.Sprintf("partition-%d", p)
				expect.Contains(t, stdout.String(), name+partitionSeparator, "the command writes the log of "+name)
			}
			expect.Contains(t, stdout.String(), messageSeparator+consumerB,
				"consumer-b consumes the messages of the partitions that move")
		})

		t.Run("exits with the status 1 when the write of a log fails", func(t *testing.T) {
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
			expect.Contains(t, stderr.String(), "the log of partition 0", "the command writes the error")
		})
	})
}

// TestCommand checks the consumer of the example, and the run of the example in the process of
// the test.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("consumer", func(t *testing.T) {
		t.Parallel()

		t.Run("partitionsOf", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name string
				give shard.Range
				want []int
			}{
				{
					name: "returns the partition of the slot that the range of a shard covers",
					give: slot(partition),
					want: []int{partition},
				},
				{name: "returns every partition for the whole range", give: shard.Range{}, want: []int{0, 1, 2, 3}},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()
					c := newConsumer(&consumerConfig{storage: fencedstore.New()})
					assert.Equal(t, c.partitionsOf(tt.give), tt.want, "the range must contain the partitions")
				})
			}
		})

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("records the tenure of an add once", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.pair(t)
					ra, rb := f.record(consumerA), f.record(consumerB)
					f.drain(t, consumerB)
					drops, adds := rb.callbacks(protocol.Drop), ra.callbacks(protocol.Add)
					assert.NotEmpty(t, drops, "consumer-b must have dropped its partitions")
					assert.NotEmpty(t, adds, "consumer-a must have taken the partitions of consumer-b")
					before := make(map[int]string, partitions)
					for p := range partitions {
						before[p] = f.assignments(p)
					}
					f.replay(t, append(drops, adds...))
					for p, want := range before {
						expect.Equal(t, f.assignments(p), want, "an add that arrives again must not record a tenure")
					}
					f.finish(t)
				})
			})

			t.Run("reads the offsets from the storage when the replica has not applied the acquisition",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						a, _ := f.pair(t)
						a.consumer.replica.Pause()
						reads := a.consumer.replica.Reads()
						<-time.After(2 * fixtureTick)
						r := f.record(consumerA)
						f.drain(t, consumerB)
						moved := make([]int, 0, partitions)
						for _, add := range r.callbacks(protocol.Add) {
							moved = append(moved, a.consumer.partitionsOf(add.Range)...)
						}
						assert.NotEmpty(t, moved, "consumer-a must have taken a partition of consumer-b")
						assert.Equal(t, a.consumer.replica.Reads(), reads,
							"the consumer must not read a replica that lacks the acquisition")
						a.consumer.replica.Resume()
						f.finish(t)
					})
				})

			t.Run("returns an error that wraps epoch.ErrFenced for a tenure that a later tenure has fenced",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						f := newFixture(t)
						adds, _ := f.moved(t)
						a := f.procs[consumerA]
						current := token(t, a, adds[0].Shard)
						later := current
						later.Epoch++
						assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
						again := adds[0].CloneKanon()
						again.Step++
						_, err := a.consumer.Open(t.Context(), &replica.OpenRequest{Request: again, Token: current})
						assert.ErrorIs(t, err, epoch.ErrFenced, "the fence must refuse the tenure")
					})
				})

			t.Run("returns an error that wraps strconv.ErrSyntax for an offset that is not a number",
				func(t *testing.T) {
					t.Parallel()
					storage := fencedstore.New()
					tok := fence.Token{Shard: sid, Epoch: 1}
					assert.NoError(t, storage.Acquire(t.Context(), tok), "the tenure acquires the fence")
					assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
						tx.Put(string(offsetPrefix)+strconv.Itoa(partition), garbled)
						return nil
					}), "the offset commits")
					c := newConsumer(&consumerConfig{storage: storage})
					_, err := c.Open(t.Context(), &replica.OpenRequest{
						Request: &protocol.Request{Shard: sid, Range: slot(partition), Call: protocol.Add},
						Token:   tok,
					})
					assert.ErrorIs(t, err, strconv.ErrSyntax, "Open must refuse an offset that is not a number")
				})

			t.Run("returns a work that consumes every message of every partition in order", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					f.start(t, consumerA)
					f.finish(t)
					for p, entries := range f.logs() {
						for _, entry := range entries {
							expect.HasSuffix(t, entry, messageSeparator+consumerA,
								"the one process must consume each message of partition "+strconv.Itoa(p))
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
						later := token(t, f.procs[consumerA], add.Shard)
						later.Epoch++
						assert.NoError(t, f.storage.Acquire(t.Context(), later), "the later tenure acquires the fence")
					}
					before := f.logs()
					<-time.After(stopFor)
					after := f.logs()
					for _, p := range fenced {
						expect.InRange(
							t,
							len(before[p]),
							0,
							fixtureMessages-1,
							"a fenced partition must have a message left",
						)
						expect.Equal(t, after[p], before[p], "the consumption of a fenced tenure must stop")
					}
				})
			})

			t.Run("returns a work that stops at the drop of the tenure", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					adds, dropped := f.moved(t)
					for _, add := range adds {
						_, err := f.procs[consumerA].runner.DropShard(
							t.Context(),
							&replica.DropShardRequest{Request: add},
						)
						assert.NoError(t, err, "the runner drops the tenure")
					}
					before := f.logs()
					<-time.After(stopFor)
					after := f.logs()
					for _, p := range dropped {
						expect.InRange(
							t,
							len(before[p]),
							0,
							fixtureMessages-1,
							"a dropped partition must have a message left",
						)
						expect.Equal(t, after[p], before[p], "the consumption of a dropped tenure must stop")
					}
				})
			})
		})
	})

	t.Run("offsetOf", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the committed offset of the partition", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(offsetPrefix)+strconv.Itoa(partition), committed)
				return nil
			}), "the offset commits")
			offset, err := offsetOf(storage, partition)
			assert.NoError(t, err, "offsetOf must read the committed offset")
			assert.Equal(t, offset, 2, "offsetOf must return the committed offset")
		})

		t.Run("returns 0 for a partition without a commit", func(t *testing.T) {
			t.Parallel()
			offset, err := offsetOf(fencedstore.New(), partition)
			assert.NoError(t, err, "offsetOf must read a partition without a commit")
			assert.Equal(t, offset, 0, "a partition without a commit must have the offset 0")
		})

		t.Run("returns an error that wraps strconv.ErrSyntax for an offset that is not a number", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(offsetPrefix)+strconv.Itoa(partition), garbled)
				return nil
			}), "the offset commits")
			_, err := offsetOf(storage, partition)
			assert.ErrorIs(t, err, strconv.ErrSyntax, "offsetOf must refuse an offset that is not a number")
		})
	})

	t.Run("logOf", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the entry of each message of the partition in order", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(logPrefix)+strconv.Itoa(partition), logged)
				return nil
			}), "the log commits")
			assert.Equal(t, logOf(storage, partition), []string{"1:consumer-a", "2:consumer-b"},
				"logOf must return the entries of the log")
		})

		t.Run("returns no entry for a partition without a consumed message", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, logOf(fencedstore.New(), partition), "a partition without a message must have no entry")
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes each message of each partition once in order", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
				assert.Length(t, lines, partitions, "run writes one line per partition")
				for p, line := range lines {
					name, list, ok := strings.Cut(line, partitionSeparator)
					assert.True(t, ok, "the line of a partition starts with its name")
					expect.Equal(
						t,
						name,
						fmt.Sprintf("partition-%d", p),
						"the lines are in the order of the partitions",
					)
					entries := strings.Fields(list)
					assert.Length(t, entries, messages, "every message of "+name+" is consumed once")
					for i, entry := range entries {
						number, _, _ := strings.Cut(entry, messageSeparator)
						expect.Equal(t, number, strconv.Itoa(i+1), "the messages of "+name+" are consumed in order")
					}
				}
			})
		})

		t.Run("consumes the first message of every partition on consumer-a", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				for line := range strings.Lines(out.String()) {
					_, list, _ := strings.Cut(line, partitionSeparator)
					expect.HasPrefix(t, list, "1"+messageSeparator+consumerA, "consumer-a consumes the first message")
				}
			})
		})

		t.Run("consumes the messages of the partitions that move on consumer-b", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.Contains(t, out.String(), messageSeparator+consumerB, "consumer-b must consume a message")
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

		t.Run("returns the cause of its context when the context ends before every message has been consumed",
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

		t.Run("returns the error of the write of a log", func(t *testing.T) {
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
// of the bubble waits. The consumer of the process consumes fixtureMessages messages per
// partition, one per fixtureTick, through a replica.Runner. The process stops when t ends.
func (f *fixture) start(t *testing.T, name string) *proc {
	t.Helper()
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	c := newConsumer(&consumerConfig{
		storage:  f.storage,
		clock:    hlc.New(1),
		node:     node.ID,
		messages: fixtureMessages,
		tick:     fixtureTick,
	})
	runner := replica.NewRunner(c)
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
	pr := &proc{Process: p, consumer: c, runner: runner}
	f.procs[name] = pr
	synctest.Wait()
	return pr
}

// pair starts consumer-a, waits until every partition has a consumed message, and then starts
// consumer-b. The moves of the first placement on consumer-a have then ended, so the
// controller moves half of the partitions to consumer-b in one pass.
func (f *fixture) pair(t *testing.T) (a, b *proc) {
	t.Helper()
	a = f.start(t, consumerA)
	f.await(t, func() bool { return begun(f.storage) }, "the first message of every partition")
	return a, f.start(t, consumerB)
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

// moved starts both consumers through pair. It then records the callbacks of consumer-a while
// consumer-b closes. It returns the adds of the shards of consumer-b that consumer-a has taken,
// and their partitions. It checks that consumer-a has taken a partition.
func (f *fixture) moved(t *testing.T) ([]*protocol.Request, []int) {
	t.Helper()
	f.pair(t)
	r := f.record(consumerA)
	f.drain(t, consumerB)
	adds := r.callbacks(protocol.Add)
	moved := make([]int, 0, partitions)
	for _, add := range adds {
		moved = append(moved, f.procs[consumerA].consumer.partitionsOf(add.Range)...)
	}
	assert.NotEmpty(t, moved, "consumer-a must have taken a partition of consumer-b")
	return adds, moved
}

// token returns the fencing token of the tenure of the shard id on the node of p.
func token(t *testing.T, p *proc, id shard.ID) fence.Token {
	t.Helper()
	a, ok := p.Agent(namespace)
	assert.True(t, ok, "the process has the agent of the namespace")
	res, err := a.Token(t.Context(), &replica.TokenRequest{Shard: id})
	assert.NoError(t, err, "the node has the tenure of the shard")
	return res.Token
}

// replay passes each callback of calls to the node of the callback again: an add to the
// consumer of the node, and a drop to its runner.
func (f *fixture) replay(t *testing.T, calls []*protocol.Request) {
	t.Helper()
	for _, r := range calls {
		p := f.procs[string(r.Node)]
		if r.Call == protocol.Add {
			_, err := p.consumer.Open(t.Context(), &replica.OpenRequest{Request: r, Token: token(t, p, r.Shard)})
			assert.NoError(t, err, "the consumer opens the tenure again")
			continue
		}
		_, err := p.runner.DropShard(t.Context(), &replica.DropShardRequest{Request: r})
		assert.NoError(t, err, "the runner takes the drop again")
	}
}

// assignments returns the assignments of the partition p.
func (f *fixture) assignments(p int) string {
	list, _ := f.storage.Get(string(assignmentsPrefix) + strconv.Itoa(p))
	return list
}

// logs returns the log of each partition.
func (f *fixture) logs() map[int][]string {
	ls := make(map[int][]string, partitions)
	for p := range partitions {
		ls[p] = logOf(f.storage, p)
	}
	return ls
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

// finish waits until every message of every partition has been consumed, and checks that
// each message has been consumed once, in order.
func (f *fixture) finish(t *testing.T) {
	t.Helper()
	f.await(t, func() bool { return consumed(f.storage) == partitions*fixtureMessages },
		"the last message of every partition")
	for p, entries := range f.logs() {
		assert.Length(
			t,
			entries,
			fixtureMessages,
			"every message of partition "+strconv.Itoa(p)+" must be consumed once",
		)
		for i, entry := range entries {
			number, _, _ := strings.Cut(entry, messageSeparator)
			assert.Equal(t, number, strconv.Itoa(i+1), "the messages of partition "+strconv.Itoa(p)+" must be in order")
		}
	}
}

// slot returns the range of the slot p: the positions from the number p to the number p+1,
// each as 4 bytes in big-endian order.
func slot(p int) shard.Range {
	return shard.Range{
		Start: binary.BigEndian.AppendUint32(nil, uint32(p)),
		End:   binary.BigEndian.AppendUint32(nil, uint32(p+1)),
	}
}
