// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/storetest"
)

// mainEnv makes the test binary run main in place of its tests when it has the value runMain.
// go test sets GOCOVERDIR for a test binary built with coverage. The process writes its
// coverage counters there when it exits, and the test binary merges them into its profile.
const (
	mainEnv = "TOPOLOGY_EMBEDDED_MAIN"
	runMain = "1"
)

// The lines that the tests expect in the output of the program.
const (
	// specLine is the spec that the root scope stores: the spec of process-a.
	specLine = "spec: version 1 with 4 slots\n"
	// firstRoot starts the line of the first leader of the root.
	firstRoot = "root: leads on process-a"
	// lastRoot starts the line of the leader of the root once process-a has closed.
	lastRoot = "root: leads on process-b"
	// lastCell starts the line of the host of the cell once process-a has closed.
	lastCell = "cell: hosted on process-b"
)

// The specs that the root scope of a double stores.
const (
	// validSpec is a spec with the configuration of fixed-slots.
	validSpec = `{"partitioning":{"config":{"slots":4}}}`
	// invalidSpec is a spec that does not decode.
	invalidSpec = `{`
	// invalidSlots is a spec whose configuration of fixed-slots does not decode.
	invalidSlots = `{"partitioning":{"config":{"slots":"four"}}}`
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

// cell is the cell that the doubles of the tests list.
var cell = shard.Placement{Shard: shard.ID{ID128: id.ID128{0: 0x01, 15: 0x01}}}

// refusing is an io.Writer that refuses every write.
type refusing struct{}

// Write returns errWrite.
func (refusing) Write([]byte) (int, error) {
	return 0, errWrite
}

// scopePage is what the double reader returns for one scope.
type scopePage struct {
	// state is the state of the scope.
	state store.State
	// placements lists the placements of the page.
	placements []shard.Placement
	// err is the error of the read.
	err error
	// seq is the error of the iteration of the page.
	seq error
	// close is the error of the close of the cursor.
	close error
}

// pages are the pages that the double reader returns: root for the root scope and cell for
// every other scope.
type pages struct {
	// root is the page of the root scope.
	root scopePage
	// cell is the page of the scope of a cell.
	cell scopePage
}

// cursor is the cursor of a page of the double reader.
type cursor struct {
	// page is the page of the cursor.
	page scopePage
}

// Seq yields the placements of the page, and then the error of its iteration.
func (c *cursor) Seq(context.Context) iter.Seq2[shard.Placement, error] {
	return func(yield func(shard.Placement, error) bool) {
		for _, p := range c.page.placements {
			if !yield(p, nil) {
				return
			}
		}
		if c.page.seq != nil {
			yield(shard.Placement{}, c.page.seq)
		}
	}
}

// NextPage returns no token, because the page is the last.
func (*cursor) NextPage() string {
	return ""
}

// Close returns the error of the close of the page.
func (c *cursor) Close() error {
	return c.page.close
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

		t.Run("writes the line of each event to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			expect.Contains(t, stdout.String(), specLine, "the command writes the stored spec")
			expect.Contains(t, stdout.String(), lastRoot, "the command writes the new leader of the root")
			expect.Contains(t, stdout.String(), lastCell, "the command writes the new host of the cell")
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

// TestCommand checks the run of the example in the process of the test, and its reads of the
// scopes.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes the start of each process", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				assert.HasPrefix(t, out.String(),
					"process-a: starts with the spec of 4 slots\nprocess-b: starts with the spec of 8 slots\n",
					"run must write the start of each process in order")
			})
		})

		t.Run("stores the spec of the process whose root leads first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				expect.Contains(t, out.String(), specLine, "the root scope must store the spec of process-a")
				expect.Contains(t, out.String(), firstRoot, "the root of process-a must lead first")
			})
		})

		t.Run("moves the root and the cell to process-b once process-a closes", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				expect.Contains(t, out.String(), lastRoot, "the root of process-b must lead")
				expect.Contains(t, out.String(), lastCell, "process-b must host the cell")
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

		t.Run("returns the cause of its context when the context ends before the controllers move",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithTimeoutCause(t.Context(), tick/2, errStop)
					defer cancel()
					assert.ErrorIs(
						t,
						run(ctx, newEnvironment(t, io.Discard)),
						errStop,
						"run must return the cause of its context",
					)
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

		t.Run("returns the error of observe", func(t *testing.T) {
			t.Parallel()
			_, err := await(t.Context(), hlc.New(1), newReader(t, pages{root: scopePage{err: errRead}}),
				func(view) bool { return true })
			assert.ErrorIs(t, err, errRead, "await must return the error of the read")
		})

		t.Run("returns the cause of its context when the context ends first", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeoutCause(t.Context(), tick, errStop)
				defer cancel()
				st := newReader(t, pages{
					root: scopePage{state: store.State{Spec: []byte(validSpec)}, placements: []shard.Placement{cell}},
				})
				_, err := await(ctx, hlc.New(1), st, func(view) bool { return false })
				assert.ErrorIs(t, err, errStop, "await must return the cause of its context")
			})
		})
	})

	t.Run("observe", func(t *testing.T) {
		t.Parallel()

		root := store.State{Spec: []byte(validSpec)}
		tests := []struct {
			give pages
			want error
			name string
		}{
			{
				name: "returns the error of the read of the root scope",
				give: pages{root: scopePage{err: errRead}},
				want: errRead,
			},
			{
				name: "returns the error of the iteration of the cells",
				give: pages{root: scopePage{seq: errRead}},
				want: errRead,
			},
			{
				name: "returns errNoCell for a root scope without a cell",
				give: pages{},
				want: errNoCell,
			},
			{
				name: "returns the error of the read of the scope of the cell",
				give: pages{
					root: scopePage{state: root, placements: []shard.Placement{cell}},
					cell: scopePage{err: errRead},
				},
				want: errRead,
			},
			{
				name: "returns the error of the close of the cursor of the cell",
				give: pages{
					root: scopePage{state: root, placements: []shard.Placement{cell}},
					cell: scopePage{close: errRead},
				},
				want: errRead,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := observe(t.Context(), newReader(t, tt.give))
				assert.ErrorIs(t, err, tt.want, "observe must return the error of the read")
			})
		}

		t.Run("returns an error for a spec that does not decode", func(t *testing.T) {
			t.Parallel()
			st := newReader(t, pages{
				root: scopePage{state: store.State{Spec: []byte(invalidSpec)}, placements: []shard.Placement{cell}},
			})
			_, err := observe(t.Context(), st)
			assert.HasError(t, err, "observe must refuse a spec that does not decode")
		})

		t.Run("returns an error for a configuration of fixed-slots that does not decode", func(t *testing.T) {
			t.Parallel()
			st := newReader(t, pages{
				root: scopePage{state: store.State{Spec: []byte(invalidSlots)}, placements: []shard.Placement{cell}},
			})
			_, err := observe(t.Context(), st)
			assert.HasError(t, err, "observe must refuse a configuration that does not decode")
		})
	})
}

// newReader returns a double of a store.Reader whose Read returns the page of p for each
// scope. It reports to tb.
func newReader(tb assert.TB, p pages) *storetest.ReaderStub {
	tb.Helper()
	read := func(
		_ context.Context,
		s shard.Scope,
		_ shard.NamespaceIncarnation,
		_ store.Query,
		_ page.Page,
	) (store.State, page.Cursor[shard.Placement], error) {
		sp := p.root
		if s.Cell != "" {
			sp = p.cell
		}
		return sp.state, &cursor{page: sp}, sp.err
	}
	return storetest.NewReaderStub(tb, storetest.WithReaderRead(read))
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
