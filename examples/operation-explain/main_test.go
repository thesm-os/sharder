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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/maintenance"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/membership/membershiptest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/protocoltest"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/storetest"
)

// mainEnv makes the test binary run main in place of its tests when it has the value runMain.
// go test sets GOCOVERDIR for a test binary built with coverage. The process writes its
// coverage counters there when it exits, and the test binary merges them into its profile.
const (
	mainEnv = "OPERATION_EXPLAIN_MAIN"
	runMain = "1"
)

// The lines that the tests expect in the output of the program. The split of the shards and the
// costs of the goals depend on the order of the passes of the controller.
const (
	// beforeA starts the output with the replicas of process-a before the explanation.
	beforeA = "before: process-a hosts "
	// explainA starts the line of the explanation.
	explainA = "explain: shard "
	// explainB contains the end of the line of the explanation, before the costs.
	explainB = " has 0 violations and the costs ["
	// afterA starts the line of the replicas of process-a after the explanation.
	afterA = "\nafter: process-a hosts "
)

// The values of the tests.
const (
	// intruder is a caller that the controllers do not admit.
	intruder shard.Identity = "intruder"
	// lifetimeOfTests is the lifetime of the proposals of a client of a test.
	lifetimeOfTests = time.Minute
	// cellAddress is the address of the controller of the cell of the doubles.
	cellAddress = "process-a:7000"
	// attempts is the number of the Submits of an explanation whose first proposal the
	// controller refuses.
	attempts = 2
)

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errWrite is the error of a writer that refuses every write.
	errWrite = errors.New("test: the writer refuses the write")
	// errRead is the error of a double that refuses a read.
	errRead = errors.New("test: the read fails")
	// errBusy is a refusal of class errs.Transient, such as the refusal of a proposal whose
	// shard has a transition in flight.
	errBusy = errs.WithClass(errors.New("test: the shard has a transition in flight"), errs.Transient)
	// errStale is a refusal of class errs.Conflict, such as the refusal of an explanation whose
	// base is no longer the generation of its cell.
	errStale = errs.WithClass(errors.New("test: the cell is at another generation"), errs.Conflict)
)

// The namespace of the doubles.
var (
	// incarnation is the namespace incarnation of the doubles.
	incarnation = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: 0x1a, 15: 0x01})}
	// cellKey is the shard of the cell in the root scope of the doubles.
	cellKey = shard.ID{ID128: id.ID128{0: 0x01, 15: 0xce}}
	// first is the first shard of process-a in the doubles.
	first = shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x01}}
	// layout lists the placements of the cell of the doubles: three shards on process-a and one
	// on process-b.
	layout = []shard.Placement{
		{Shard: first, Replicas: []shard.Replica{{Node: processA, Role: shard.RolePrimary}}, Epoch: 1},
		{Shard: shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x02}}, Replicas: []shard.Replica{{Node: processA}}, Epoch: 1},
		{Shard: shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x03}}, Replicas: []shard.Replica{{Node: processA}}, Epoch: 1},
		{Shard: shard.ID{ID128: id.ID128{0: 0x5d, 15: 0x04}}, Replicas: []shard.Replica{{Node: processB}}, Epoch: 1},
	}
	// explanation is the explanation that the transport of the doubles returns.
	explanation = proposal.Explanation{Costs: []float64{1}}
)

// refusing is an io.Writer that refuses every write.
type refusing struct{}

// Write returns errWrite.
func (refusing) Write([]byte) (int, error) {
	return 0, errWrite
}

// cursor is the cursor of the one page of placements of the double reader.
type cursor struct {
	// placements lists the placements of the page.
	placements []shard.Placement
}

// Seq yields the placements of the page.
func (c *cursor) Seq(context.Context) iter.Seq2[shard.Placement, error] {
	return func(yield func(shard.Placement, error) bool) {
		for _, p := range c.placements {
			if !yield(p, nil) {
				return
			}
		}
	}
}

// NextPage returns the empty string, because the page is the last.
func (*cursor) NextPage() string {
	return ""
}

// Close returns nil.
func (*cursor) Close() error {
	return nil
}

// doubles is a namespace of one cell with the view of both processes, which the doubles of a
// client serve, and the proposals that the doubles receive.
type doubles struct {
	// readErr is the error of each read of the store.
	readErr error
	// placements lists the placements of the cell.
	placements []shard.Placement
	// refusals lists the errors of the first Submits, in order. Each later Submit returns
	// explanation.
	refusals []error
	// submitted lists the proposals of the Submits, in order.
	submitted []proposal.Proposal
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

		t.Run("writes the explanation to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			expect.HasPrefix(t, stdout.String(), beforeA, "the command writes the replicas before the explanation")
			expect.Contains(t, stdout.String(), explainB, "the command writes the explanation")
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

// TestCommand checks the run of the example in the process of the test, the explanation and
// its waits for the status of the namespace.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes the explanation of a shard of process-a", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				expect.HasPrefix(t, out.String(), beforeA, "run must write process-a first")
				expect.Contains(t, out.String(), explainA, "run must write the explained shard")
				expect.Contains(t, out.String(), explainB, "run must write the violations of the constraints")
			})
		})

		t.Run("writes the replicas of each node after the explanation", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.Contains(t, out.String(), afterA, "run must write the replicas after the explanation")
				_, rest, _ := strings.Cut(out.String(), explainA)
				assert.Contains(t, rest, afterA, "the replicas after the explanation must follow it")
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
					assert.HasError(t, err, "the controller must refuse the explanation of an intruder")
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

	t.Run("explain", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give error
			name string
		}{
			{name: "submits a new proposal after a refusal of class errs.Transient", give: errBusy},
			{name: "submits a new proposal after a refusal of class errs.Conflict", give: errStale},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				d := &doubles{placements: layout, refusals: []error{tt.give}}
				_, sid, res, err := explain(t.Context(), hlc.New(1, clock.DefaultMaxOffset), d.client(t))
				assert.NoError(t, err, "explain must explain the shard once the refusal has passed")
				expect.Equal(t, sid, first, "explain must explain the first shard of process-a")
				expect.Equal(t, res.Explanation, explanation, "explain must return the explanation of the controller")
				expect.Length(t, d.submitted, attempts, "explain must submit a second proposal after the refusal")
			})
		}

		t.Run("returns errNoShard for a status in which process-a hosts no shard", func(t *testing.T) {
			t.Parallel()
			d := &doubles{placements: []shard.Placement{
				{Shard: first, Replicas: []shard.Replica{{Node: processB}, {Node: processB}}, Epoch: 1},
				{Shard: cellKey, Replicas: []shard.Replica{{Node: processB}, {Node: processB}}, Epoch: 1},
			}}
			_, _, _, err := explain(t.Context(), hlc.New(1, clock.DefaultMaxOffset), d.client(t))
			assert.ErrorIs(t, err, errNoShard, "explain must refuse a status without a shard of process-a")
		})

		t.Run("returns the error of await", func(t *testing.T) {
			t.Parallel()
			d := &doubles{readErr: errRead}
			_, _, _, err := explain(t.Context(), hlc.New(1, clock.DefaultMaxOffset), d.client(t))
			assert.ErrorIs(t, err, errRead, "explain must return the error of the read")
		})
	})

	t.Run("await", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the error of Client.Status", func(t *testing.T) {
			t.Parallel()
			d := &doubles{readErr: errRead}
			_, err := await(t.Context(), hlc.New(1, clock.DefaultMaxOffset), d.client(t), placed)
			assert.ErrorIs(t, err, errRead, "await must return the error of the read")
		})

		t.Run("returns the cause of its context when the context ends first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeoutCause(t.Context(), tick, errStop)
				defer cancel()
				_, err := await(ctx, hlc.New(1, clock.DefaultMaxOffset), (&doubles{}).client(t), placed)
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
}

// client returns a client over the doubles of d, which serve a root scope with one cell and a
// view of both processes. The transport refuses the first Submits with the errors of
// d.refusals, and returns explanation for each later Submit.
func (d *doubles) client(t *testing.T) *maintenance.Client {
	t.Helper()
	spec, err := json.Marshal(&config.Spec{Namespace: namespace, Version: 1})
	assert.NoError(t, err, "the spec of the doubles encodes")
	result, err := json.Marshal(&explanation)
	assert.NoError(t, err, "the explanation of the doubles encodes")
	cellScope := shard.Scope{Namespace: namespace, Cell: shard.CellID(ulid.Format(cellKey.ID()))}
	read := func(_ context.Context, s shard.Scope, _ shard.NamespaceIncarnation, _ store.Query, _ page.Page) (
		store.State, page.Cursor[shard.Placement], error,
	) {
		if d.readErr != nil {
			return store.State{}, nil, d.readErr
		}
		head := store.Head{Incarnation: incarnation}
		if s.Cell == "" {
			root := store.State{Spec: spec, Head: head}
			return root, &cursor{placements: []shard.Placement{{Shard: cellKey}}}, nil
		}
		cell := store.State{Cell: shard.Cell{ID: cellScope.Cell, Controller: cellAddress}, Head: head}
		return cell, &cursor{placements: d.placements}, nil
	}
	watch := func(context.Context, shard.Scope, shard.NamespaceIncarnation) iter.Seq2[membership.View, error] {
		return func(yield func(membership.View, error) bool) {
			yield(membership.View{
				Scope:       cellScope,
				Nodes:       []shard.Node{{ID: processA}, {ID: processB}},
				Incarnation: incarnation,
			}, nil)
		}
	}
	submit := func(_ context.Context, _ *shard.Cell, p *proposal.Proposal) (protocol.Receipt, error) {
		d.submitted = append(d.submitted, *p)
		if len(d.submitted) <= len(d.refusals) {
			return protocol.Receipt{}, d.refusals[len(d.submitted)-1]
		}
		return protocol.Receipt{Result: result}, nil
	}
	c, err := maintenance.New(&maintenance.Config{
		Store:      storetest.NewReaderStub(t, storetest.WithReaderRead(read)),
		Membership: membershiptest.NewMembershipStub(t, membershiptest.WithMembershipWatch(watch)),
		Transport:  protocoltest.NewControllerClientStub(t, protocoltest.WithControllerClientSubmit(submit)),
		Clock:      hlc.New(1, clock.DefaultMaxOffset),
		Rand:       seeded.New(1),
		Lifetime:   lifetimeOfTests,
	})
	assert.NoError(t, err, "the client of a complete config builds")
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
