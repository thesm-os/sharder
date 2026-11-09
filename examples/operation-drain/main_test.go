// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/storetest"
)

// mainEnv makes the test binary run main in place of its tests when it has the value runMain.
// go test sets GOCOVERDIR for a test binary built with coverage. The process writes its
// coverage counters there when it exits, and the test binary merges them into its profile.
const (
	mainEnv = "OPERATION_DRAIN_MAIN"
	runMain = "1"
)

// The lines that the tests expect in the output of the program. The split of the shards before
// the drain depends on the order of the passes of the controller.
const (
	// beforeA starts the output with the replicas of process-a before the drain.
	beforeA = "before: process-a hosts "
	// beforeB starts the line of the replicas of process-b before the drain.
	beforeB = "\nbefore: process-b hosts "
	// drainLine is the decision of the budget for a disruption of process-a, which still hosts
	// its replicas when the drain applies.
	drainLine = "drain: process-a, the budget allows a disruption: false\n"
	// drainedLine is process-a once it has drained.
	drainedLine = "after: process-a hosts 0 of the 4 shards and drains\n"
	// movedLine is process-b once it hosts the replicas of process-a.
	movedLine = "after: process-b hosts 4 of the 4 shards\n"
)

// The values of the tests.
const (
	// intruder is a caller that the controllers do not admit.
	intruder shard.Identity = "intruder"
	// lifetimeOfTests is the lifetime of the proposals of a client of a test.
	lifetimeOfTests = time.Minute
)

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errWrite is the error of a writer that refuses every write.
	errWrite = errors.New("test: the writer refuses the write")
	// errRead is the error of a double that refuses a read.
	errRead = errors.New("test: the read fails")
)

// incarnation is the namespace incarnation of the double reader.
var incarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x01})}

// refusing is an io.Writer that refuses every write.
type refusing struct{}

// Write returns errWrite.
func (refusing) Write([]byte) (int, error) {
	return 0, errWrite
}

// empty is the cursor of a page without placements.
type empty struct{}

// Seq returns an empty sequence.
func (empty) Seq(context.Context) iter.Seq2[shard.Placement, error] {
	return func(func(shard.Placement, error) bool) {}
}

// NextPage returns the empty string, because the page is the last.
func (empty) NextPage() string {
	return ""
}

// Close returns nil.
func (empty) Close() error {
	return nil
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

		t.Run("writes the replicas of each node to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			expect.HasPrefix(t, stdout.String(), beforeA, "the command writes the replicas before the drain")
			expect.Contains(t, stdout.String(), drainedLine, "the command writes the drained node")
		})

		t.Run("exits with the status 1 when the write of the output fails", func(t *testing.T) {
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
			expect.Contains(t, stderr.String(), "the output", "the command writes the error")
		})
	})
}

// TestCommand checks the run of the example in the process of the test, and its waits for the
// status of the namespace.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes the replicas of each node before the drain", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				expect.HasPrefix(t, out.String(), beforeA, "run must write process-a first")
				expect.Contains(t, out.String(), beforeB, "run must write process-b second")
			})
		})

		t.Run("writes the decision of the budget for a disruption of the drained node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.Contains(t, out.String(), drainLine, "the budget must defer a disruption of a busy node")
			})
		})

		t.Run("moves every replica of the drained node to the other node", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				expect.Contains(t, out.String(), drainedLine, "process-a must drain")
				expect.Contains(t, out.String(), movedLine, "process-b must host every replica")
			})
		})

		t.Run("returns an error that wraps maintenance.ErrConfig for an environment without a source of randomness",
			func(t *testing.T) {
				t.Parallel()
				env := newEnvironment(t, io.Discard)
				env.rand = nil
				assert.ErrorIs(t, run(t.Context(), env), maintenance.ErrConfig, "the client must refuse a nil source")
			})

		t.Run("returns an error that wraps config.ErrUnknown for a registry without the built-in strategies",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					env := newEnvironment(t, io.Discard)
					env.registry = config.NewRegistry(builtin.Support())
					assert.ErrorIs(t, run(t.Context(), env), config.ErrUnknown,
						"the spec must not resolve without the preset")
				})
			})

		t.Run("returns an error that wraps embedded.ErrConfig for an environment without a logger",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					env := newEnvironment(t, io.Discard)
					env.logger = nil
					assert.ErrorIs(t, run(t.Context(), env), embedded.ErrConfig, "the process must refuse a nil logger")
				})
			})

		t.Run("returns an error of class errs.Denied for a caller that the controllers do not admit",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					env := newEnvironment(t, io.Discard)
					env.caller = intruder
					err := run(t.Context(), env)
					assert.HasError(t, err, "the controller must refuse the drain of an intruder")
					assert.Equal(t, errs.Classify(err), errs.Denied, "the refusal must classify as errs.Denied")
				})
			})

		t.Run("returns an error that wraps context.Canceled for a context that has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				assert.ErrorIs(t, run(ctx, newEnvironment(t, io.Discard)), context.Canceled,
					"run must return the error of the first read of the status")
			})
		})

		t.Run("returns the error of the write of the output", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				assert.ErrorIs(t, run(t.Context(), newEnvironment(t, refusing{})), errWrite,
					"run must return the error of the writer")
			})
		})
	})

	t.Run("await", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Client.Status", func(t *testing.T) {
			t.Parallel()
			_, err := await(t.Context(), hlc.New(1, clock.DefaultMaxOffset), clientOf(t, errRead), placed)
			assert.ErrorIs(t, err, errRead, "await must return the error of the read")
		})

		t.Run("returns the cause of its context when the context ends first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeoutCause(t.Context(), tick, errStop)
				defer cancel()
				_, err := await(ctx, hlc.New(1, clock.DefaultMaxOffset), clientOf(t, nil), placed)
				assert.ErrorIs(t, err, errStop, "await must return the cause of its context")
			})
		})
	})

	t.Run("placed", func(t *testing.T) {
		t.Parallel()

		most, one := make([]maintenance.Replica, slots-1), make([]maintenance.Replica, 1)
		tests := []struct {
			give maintenance.StatusResult
			name string
			want bool
		}{
			{name: "reports false for a status without a cell"},
			{
				name: "reports false for a cell with a transition in flight",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes:       []maintenance.NodeStatus{{Replicas: most}, {Replicas: one}},
					Transitions: []store.Transition{{}},
				}}},
			},
			{
				name: "reports false for a cell with one node",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Replicas: make([]maintenance.Replica, slots)}},
				}}},
			},
			{
				name: "reports false for a cell with a shard without a replica",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Replicas: most}, {}},
				}}},
			},
			{
				name: "reports true for a cell that places every shard",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Replicas: most}, {Replicas: one}},
				}}},
				want: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, placed(&tt.give), tt.want, "placed must report a cell that places every shard")
			})
		}
	})

	t.Run("drained", func(t *testing.T) {
		t.Parallel()

		a := shard.Node{ID: processA}
		tests := []struct {
			give maintenance.StatusResult
			name string
			want bool
		}{
			{name: "reports false for a status without a cell"},
			{
				name: "reports false for a cell with a transition in flight",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes:       []maintenance.NodeStatus{{Node: a, Draining: true}},
					Transitions: []store.Transition{{}},
				}}},
			},
			{
				name: "reports false for a draining node with a replica",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Replicas: []maintenance.Replica{{}}, Node: a, Draining: true}},
				}}},
			},
			{
				name: "reports false for a cell without process-a",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Node: shard.Node{ID: processB}}},
				}}},
			},
			{
				name: "reports true for a draining node without replicas",
				give: maintenance.StatusResult{Cells: []maintenance.CellStatus{{
					Nodes: []maintenance.NodeStatus{{Node: a, Draining: true}},
				}}},
				want: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, drained(&tt.give), tt.want, "drained must report the end of the drain")
			})
		}
	})
}

// clientOf returns a client over a reader of a namespace without a cell. Each read fails with
// readErr when readErr is not nil.
func clientOf(tb assert.TB, readErr error) *maintenance.Client {
	tb.Helper()
	spec, err := json.Marshal(&config.Spec{Namespace: namespace, Version: 1})
	assert.NoError(tb, err, "the spec of the double encodes")
	read := func(context.Context, shard.Scope, shard.NamespaceIncarnation, store.Query, page.Page) (
		store.State, page.Cursor[shard.Placement], error,
	) {
		if readErr != nil {
			return store.State{}, nil, readErr
		}
		return store.State{Spec: spec, Head: store.Head{Incarnation: incarnation}}, empty{}, nil
	}
	c, err := maintenance.New(&maintenance.Config{
		Store:      storetest.NewReaderStub(tb, storetest.WithReaderRead(read)),
		Membership: membershiptest.NewMembershipStub(tb),
		Transport:  protocoltest.NewControllerClientStub(tb),
		Clock:      hlc.New(1, clock.DefaultMaxOffset),
		Rand:       seeded.New(1),
		Lifetime:   lifetimeOfTests,
	})
	assert.NoError(tb, err, "the client of a complete config builds")
	return c
}

// newEnvironment returns the environment of a run of the example that writes to out. Inside a
// bubble of testing/synctest, the clock reads the virtual time of the bubble.
func newEnvironment(t *testing.T, out io.Writer) *environment {
	t.Helper()
	reg := config.NewRegistry(builtin.Support())
	assert.NoError(t, builtin.Register(reg), "the built-ins register")
	return &environment{
		out:      out,
		clock:    hlc.New(1, clock.DefaultMaxOffset),
		rand:     seeded.New(1),
		registry: reg,
		logger:   slog.New(slog.DiscardHandler),
		caller:   operator,
	}
}
