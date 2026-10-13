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
	"strconv"
	"strings"
	"testing"
	"testing/synctest"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand/seeded"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// mainEnv makes the test binary run main in place of its tests when it has the value runMain.
// go test sets GOCOVERDIR for a test binary built with coverage. The process writes its
// coverage counters there when it exits, and the test binary merges them into its profile.
const (
	mainEnv = "PRESET_ACTOR_ENTITIES_MAIN"
	runMain = "1"
)

// The parts of the output of the program.
const (
	// accountSeparator separates the name of an account from its deposits.
	accountSeparator = ": "
	// depositSeparator separates the number of a deposit from the node that served it.
	depositSeparator = ":"
	// redirectsPrefix starts the last line of the output.
	redirectsPrefix = "redirects: "
)

// deposited is the deposits of two deposits that a spec of depositsOf writes.
const deposited = "1:node-a 2:node-b"

// The errors of the tests.
var (
	// errStop is the cause with which a test ends a context.
	errStop = errors.New("test: the test stops")
	// errWrite is the error of a writer that refuses every write.
	errWrite = errors.New("test: the writer refuses the write")
	// errRead is the error of a double that refuses a read.
	errRead = errors.New("test: the read fails")
)

// The shards of the tests.
var (
	// cell is a cell that the doubles of the tests list.
	cell = shard.Placement{Shard: shard.ID{ID128: id.ID128{0: 0x01, 15: 0x01}}}
	// sid is the shard of a tenure that a test opens without a process.
	sid = shard.ID{ID128: id.ID128{0: 0x02, 15: 0x01}}
)

// refusing is an io.Writer that refuses every write.
type refusing struct{}

// Write returns errWrite.
func (refusing) Write([]byte) (int, error) {
	return 0, errWrite
}

// countdown is an io.Writer that accepts n writes and refuses every later write.
type countdown struct {
	// n is the number of the writes that countdown still accepts.
	n int
}

// Write accepts p while n is above zero, and returns errWrite after.
func (c *countdown) Write(p []byte) (int, error) {
	if c.n == 0 {
		return 0, errWrite
	}
	c.n--
	return len(p), nil
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

// reader is a store.Reader of the tests. It returns root for the root scope and cell for every
// other scope.
type reader struct {
	store.Reader
	// root is the page of the root scope.
	root scopePage
	// cell is the page of the scope of a cell.
	cell scopePage
}

// Read returns the page of the scope s.
func (r *reader) Read(_ context.Context, s shard.Scope, _ shard.NamespaceIncarnation, _ store.Query, _ page.Page) (
	store.State, page.Cursor[shard.Placement], error,
) {
	p := r.root
	if s.Cell != "" {
		p = r.cell
	}
	return p.state, &cursor{page: p}, p.err
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

// fixedRouter is a router of the tests. Route returns err for every key, and Refresh returns
// refresh.
type fixedRouter struct {
	// err is the error of Route.
	err error
	// refresh is the error of Refresh.
	refresh error
}

// Route returns the error of f.
func (f *fixedRouter) Route(context.Context, *router.RouteRequest) (router.RouteResult, error) {
	return router.RouteResult{}, f.err
}

// Refresh returns the error of the refresh of f.
func (f *fixedRouter) Refresh(context.Context, *router.RefreshRequest) (router.RefreshResult, error) {
	return router.RefreshResult{}, f.refresh
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

		t.Run("writes the deposits of each account to os.Stdout", func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), self)
			cmd.Env = append(os.Environ(), mainEnv+"="+runMain)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			assert.NoError(t, cmd.Run(), "the command runs: "+stderr.String())
			for _, name := range names {
				expect.Contains(t, stdout.String(), name+accountSeparator, "the command writes the deposits of "+name)
			}
			expect.Contains(t, stdout.String(), redirectsPrefix, "the command writes the redirects")
		})

		t.Run("exits with the status 1 when the write of the deposits fails", func(t *testing.T) {
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
			expect.Contains(t, stderr.String(), "the deposits of "+names[0], "the command writes the error")
		})
	})
}

// TestCommand checks the bank and the server of the example, and the run of the example in the
// process of the test.
func TestCommand(t *testing.T) {
	t.Parallel()

	t.Run("bank", func(t *testing.T) {
		t.Parallel()

		t.Run("accountsOf", func(t *testing.T) {
			t.Parallel()

			t.Run("returns every account for the whole range", func(t *testing.T) {
				t.Parallel()
				sys := newSystem(t)
				b := newBank(&bankConfig{storage: sys.storage, partitioner: sys.part, node: nodeA})
				assert.Equal(t, b.accountsOf(shard.Range{}), names, "the whole range must contain every account")
			})
		})

		t.Run("Open", func(t *testing.T) {
			t.Parallel()

			t.Run("records the tenure of an add once", func(t *testing.T) {
				t.Parallel()
				sys := newSystem(t)
				tok := fence.Token{Shard: sid, Epoch: 1}
				assert.NoError(t, sys.storage.Acquire(t.Context(), tok), "the tenure acquires the fence")
				b := newBank(&bankConfig{storage: sys.storage, partitioner: sys.part, node: nodeA})
				activations := string(activationsPrefix) + ulid.Format(sid.ID())
				observe := func() string {
					list, _ := sys.storage.Get(activations)
					return list
				}
				assert.Idempotent(
					t,
					func(req *replica.OpenRequest) error {
						_, err := b.Open(t.Context(), req)
						return err
					},
					&replica.OpenRequest{Request: &protocol.Request{Shard: sid, Call: protocol.Add}, Token: tok},
					observe,
					"an add that arrives again must not record a tenure",
				)
				assert.Equal(t, observe(), "node-a@1", "the activations must list the tenure")
			})

			t.Run("loads the deposits from the storage when the replica has not applied the acquisition",
				func(t *testing.T) {
					t.Parallel()
					sys := newSystem(t)
					b := newBank(&bankConfig{storage: sys.storage, partitioner: sys.part, node: nodeA})
					b.replica.Pause()
					assert.NoError(t, sys.storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
						tx.Put(string(depositsPrefix)+names[0], "1:node-b")
						return nil
					}), "the deposit of the earlier tenure commits")
					tok := fence.Token{Shard: sid, Epoch: 2}
					assert.NoError(t, sys.storage.Acquire(t.Context(), tok), "the tenure acquires the fence")
					_, err := b.Open(t.Context(), &replica.OpenRequest{
						Request: &protocol.Request{Shard: sid, Call: protocol.Add},
						Token:   tok,
					})
					assert.NoError(t, err, "the bank opens the tenure")
					expect.Equal(t, b.replica.Reads(), 0, "the bank must not read a replica that lacks the acquisition")
					srv := &server{bank: b}
					res, err := srv.Deposit(t.Context(), &depositRequest{account: names[0], shard: sid, epoch: 2})
					assert.NoError(t, err, "the server serves the deposit")
					expect.Equal(t, res.deposit, 2, "the deposit must follow the deposit of the earlier tenure")
					expect.Equal(t, depositsOf(sys.storage, names[0]), []string{"1:node-b", "2:node-a"},
						"the deposits must keep the deposit of the earlier tenure")
				})

			t.Run("returns an error that wraps epoch.ErrFenced for a tenure that a later tenure has fenced",
				func(t *testing.T) {
					t.Parallel()
					sys := newSystem(t)
					assert.NoError(t, sys.storage.Acquire(t.Context(), fence.Token{Shard: sid, Epoch: 2}),
						"the later tenure acquires the fence")
					b := newBank(&bankConfig{storage: sys.storage, partitioner: sys.part, node: nodeA})
					_, err := b.Open(t.Context(), &replica.OpenRequest{
						Request: &protocol.Request{Shard: sid, Call: protocol.Add},
						Token:   fence.Token{Shard: sid, Epoch: 1},
					})
					assert.ErrorIs(t, err, epoch.ErrFenced, "the fence must refuse the tenure")
				})

			t.Run("returns a work that ends the activation once its context ends", func(t *testing.T) {
				t.Parallel()
				b, res := activate(t, newSystem(t))
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				assert.NoError(t, res.Run(ctx), "the work returns once its context ends")
				_, active := b.active[sid]
				assert.False(t, active, "the work must end the activation of the shard")
			})
		})
	})

	t.Run("server", func(t *testing.T) {
		t.Parallel()

		t.Run("Deposit", func(t *testing.T) {
			t.Parallel()

			t.Run("appends a deposit to an account of an active shard", func(t *testing.T) {
				t.Parallel()
				sys := newSystem(t)
				b, _ := activate(t, sys)
				srv := &server{bank: b}
				res, err := srv.Deposit(t.Context(), &depositRequest{account: names[0], shard: sid, epoch: 1})
				assert.NoError(t, err, "the server must serve the deposit")
				expect.Equal(t, res.deposit, 1, "the deposit must be the first of the account")
				expect.Equal(t, depositsOf(sys.storage, names[0]), []string{"1:node-a"},
					"the deposits must list the deposit")
			})

			t.Run("returns a NotOwnerError with the observation of the node for an account on another node",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						sys := newSystem(t)
						a := startA(t, sys)
						b, err := sys.start(t.Context(), nodeB)
						assert.NoError(t, err, "node-b starts")
						t.Cleanup(func() { <-b.Done() })
						assert.NoError(t, settle(t.Context(), sys.env.clock, sys.st, a.router, nodeB, slots/2),
							"half of the shards settle on node-b")
						moved := 0
						for _, name := range names {
							res, err := a.router.Route(t.Context(), &router.RouteRequest{Key: []byte(name)})
							assert.NoError(t, err, "the router of node-a routes "+name)
							r := res.Route
							assert.NotEmpty(t, r.Replicas, "the route of "+name+" lists its writer")
							if r.Replicas[0].Node != nodeB {
								continue
							}
							moved++
							req := &depositRequest{account: name, shard: r.Shard, epoch: r.Epoch}
							_, err = a.server.Deposit(t.Context(), req)
							e := assert.ErrorAs[*router.NotOwnerError](
								t,
								err,
								"node-a must refuse the deposit of "+name,
							)
							o, err := a.server.agent.Observed(t.Context(), &replica.ObservedRequest{})
							assert.NoError(t, err, "the agent of node-a has an observation")
							expect.Equal(t, e.Shard, r.Shard, "the error must have the shard of the request")
							expect.Equal(t, e.Epoch, r.Epoch, "the error must have the epoch of the request")
							expect.Equal(t, e.Cell, o.Cell, "the error must have the cell of the node")
							expect.Equal(t, e.Generation, o.Generation,
								"the error must have the generation that the node has observed")
							expect.Equal(t, e.Incarnation, o.Incarnation,
								"the error must have the namespace incarnation of the node")
						}
						assert.NotEqual(t, moved, 0, "node-b must be the writer of an account")
					})
				})

			t.Run("returns a NotOwnerError for the epoch of another tenure", func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					a := startA(t, newSystem(t))
					res, err := a.router.Route(t.Context(), &router.RouteRequest{Key: []byte(names[0])})
					assert.NoError(t, err, "the router of node-a routes the account")
					r := res.Route
					req := &depositRequest{account: names[0], shard: r.Shard, epoch: r.Epoch + 1}
					_, err = a.server.Deposit(t.Context(), req)
					e := assert.ErrorAs[*router.NotOwnerError](t, err, "the server must refuse another epoch")
					expect.Equal(t, e.Epoch, r.Epoch+1, "the error must have the epoch of the request")
				})
			})

			t.Run("returns an error that wraps replica.ErrNotServing for a refusal after the run of the agent",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						sys := newSystem(t)
						ctx, cancel := context.WithCancel(t.Context())
						a, err := sys.start(ctx, nodeA)
						assert.NoError(t, err, "node-a starts")
						cancel()
						<-a.Done()
						req := &depositRequest{account: names[0], shard: sid, epoch: 1}
						_, err = a.server.Deposit(t.Context(), req)
						assert.ErrorIs(t, err, replica.ErrNotServing, "the server must return the error of the agent")
					})
				})

			t.Run("returns an error that wraps epoch.ErrFenced once a later tenure has acquired the fence",
				func(t *testing.T) {
					t.Parallel()
					sys := newSystem(t)
					b, _ := activate(t, sys)
					assert.NoError(t, sys.storage.Acquire(t.Context(), fence.Token{Shard: sid, Epoch: 2}),
						"the later tenure acquires the fence")
					srv := &server{bank: b}
					_, err := srv.Deposit(t.Context(), &depositRequest{account: names[0], shard: sid, epoch: 1})
					assert.ErrorIs(t, err, epoch.ErrFenced, "the fence must refuse the deposit")
				})

			t.Run("keeps the deposits of the activation when the write of a deposit fails", func(t *testing.T) {
				t.Parallel()
				b, _ := activate(t, newSystem(t))
				srv := &server{bank: b}
				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(errStop)
				_, err := srv.Deposit(ctx, &depositRequest{account: names[0], shard: sid, epoch: 1})
				assert.ErrorIs(t, err, errStop, "the write must fail with the cause of its context")
				res, err := srv.Deposit(t.Context(), &depositRequest{account: names[0], shard: sid, epoch: 1})
				assert.NoError(t, err, "the server serves the next deposit")
				assert.Equal(t, res.deposit, 1, "a deposit whose write failed must not count")
			})
		})
	})

	t.Run("system", func(t *testing.T) {
		t.Parallel()

		t.Run("start", func(t *testing.T) {
			t.Parallel()

			t.Run("returns the cause of its context when the context ends before the process serves",
				func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancelCause(t.Context())
						cancel(errStop)
						_, err := newSystem(t).start(ctx, nodeA)
						assert.ErrorIs(t, err, errStop, "start must return the cause of its context")
					})
				})
		})
	})

	t.Run("client", func(t *testing.T) {
		t.Parallel()

		t.Run("deposit", func(t *testing.T) {
			t.Parallel()

			t.Run("returns an error that wraps the error of the route of the account", func(t *testing.T) {
				t.Parallel()
				c := &client{router: &fixedRouter{err: router.ErrNoRoute}}
				err := c.deposit(t.Context(), names[0], func() {})
				assert.ErrorIs(t, err, router.ErrNoRoute, "deposit must return the error of the router")
			})
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Parallel()

		t.Run("writes each deposit of each account once in order", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
				assert.Length(t, lines, len(names)+1, "run writes one line per account and the redirects")
				for i, name := range names {
					got, deposits, ok := strings.Cut(lines[i], accountSeparator)
					assert.True(t, ok, "the line of an account starts with its name")
					expect.Equal(t, got, name, "the lines are in the order of the accounts")
					entries := strings.Fields(deposits)
					assert.Length(t, entries, rounds, "every deposit of "+name+" must appear once")
					for j, entry := range entries {
						number, _, _ := strings.Cut(entry, depositSeparator)
						expect.Equal(t, number, strconv.Itoa(j+1), "the deposits of "+name+" are in order")
					}
				}
			})
		})

		t.Run("serves the first deposit of every account on node-a", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				for _, name := range names {
					expect.Contains(t, out.String(), name+accountSeparator+"1"+depositSeparator+string(nodeA),
						"node-a serves the first deposit of "+name)
				}
			})
		})

		t.Run("routes again the second deposit of each account that moves to node-b", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				var out bytes.Buffer
				assert.NoError(t, run(t.Context(), newEnvironment(t, &out)), "run runs the example")
				moved := strings.Count(out.String(), "2"+depositSeparator+string(nodeB))
				assert.NotEqual(t, moved, 0, "node-b must serve the second deposit of an account")
				assert.Contains(t, out.String(), redirectsPrefix+strconv.Itoa(moved)+"\n",
					"router.Do must route the second deposit of each moved account again")
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

		t.Run("returns an error that wraps context.Canceled when its context ends before the shards settle",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					assert.ErrorIs(
						t,
						run(ctx, newEnvironment(t, io.Discard)),
						context.Canceled,
						"run must return the error of the read of the cells",
					)
				})
			})

		t.Run("returns the error of the write of the deposits", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				assert.ErrorIs(t, run(t.Context(), newEnvironment(t, refusing{})), errWrite,
					"run must return the error of the writer")
			})
		})

		t.Run("returns the error of the write of the redirects", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				err := run(t.Context(), newEnvironment(t, &countdown{n: len(names)}))
				assert.ErrorIs(t, err, errWrite, "run must return the error of the writer")
				assert.Contains(t, err.Error(), "the redirects", "the error must name the redirects")
			})
		})
	})

	t.Run("settle", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			give *reader
			want error
			name string
		}{
			{
				name: "returns the error of the read of the root scope",
				give: &reader{root: scopePage{err: errRead}},
				want: errRead,
			},
			{
				name: "returns the error of the iteration of the cells",
				give: &reader{root: scopePage{seq: errRead}},
				want: errRead,
			},
			{
				name: "returns the error of the close of the cursor of the cells",
				give: &reader{root: scopePage{placements: []shard.Placement{cell}, close: errRead}},
				want: errRead,
			},
			{
				name: "returns errNoCell for a root scope without a cell",
				give: &reader{},
				want: errNoCell,
			},
			{
				name: "returns the error of the read of the scope of the cell",
				give: &reader{root: scopePage{placements: []shard.Placement{cell}}, cell: scopePage{err: errRead}},
				want: errRead,
			},
			{
				name: "returns the error of the iteration of the placements of the cell",
				give: &reader{root: scopePage{placements: []shard.Placement{cell}}, cell: scopePage{seq: errRead}},
				want: errRead,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				err := settle(t.Context(), hlc.New(1), tt.give, &fixedRouter{}, nodeA, 0)
				assert.ErrorIs(t, err, tt.want, "settle must return the error of the read")
			})
		}

		t.Run("returns the error of the refresh of the router", func(t *testing.T) {
			t.Parallel()
			st := &reader{root: scopePage{placements: []shard.Placement{cell}}}
			err := settle(t.Context(), hlc.New(1), st, &fixedRouter{refresh: errRead}, nodeA, 0)
			assert.ErrorIs(t, err, errRead, "settle must return the error of the refresh")
		})

		t.Run("returns nil once the router has applied the generation of the cell", func(t *testing.T) {
			t.Parallel()
			st := &reader{root: scopePage{placements: []shard.Placement{cell}}}
			assert.NoError(t, settle(t.Context(), hlc.New(1), st, &fixedRouter{}, nodeA, 0),
				"settle must return once the cell has settled")
		})

		t.Run("returns the cause of its context when the context ends before the cell settles", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(errStop)
			st := &reader{root: scopePage{placements: []shard.Placement{cell}}}
			assert.ErrorIs(t, settle(ctx, hlc.New(1), st, &fixedRouter{}, nodeA, 1), errStop,
				"settle must return the cause of its context")
		})
	})

	t.Run("depositsOf", func(t *testing.T) {
		t.Parallel()

		t.Run("returns the entry of each deposit of the account in order", func(t *testing.T) {
			t.Parallel()
			storage := fencedstore.New()
			assert.NoError(t, storage.Update(t.Context(), func(tx *fencedstore.Tx) error {
				tx.Put(string(depositsPrefix)+names[0], deposited)
				return nil
			}), "the deposits commit")
			assert.Equal(t, depositsOf(storage, names[0]), []string{"1:node-a", "2:node-b"},
				"depositsOf must return the entries of the deposits")
		})

		t.Run("returns no entry for an account without a deposit", func(t *testing.T) {
			t.Parallel()
			assert.Empty(t, depositsOf(fencedstore.New(), names[0]), "an account without a deposit must have no entry")
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

// newSystem returns a system without processes, with the environment of a run that writes to
// io.Discard and the partitioner of the spec of the namespace. Its store closes when t ends.
func newSystem(t *testing.T) *system {
	t.Helper()
	env := newEnvironment(t, io.Discard)
	st := memory.New(env.clock)
	t.Cleanup(func() { assert.NoError(t, st.Close(), "the store closes") })
	part, err := partitioner(env.registry, st)
	assert.NoError(t, err, "the partitioner of the spec builds")
	return &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork(), part: part}
}

// startA starts node-a under the context of t, and waits until node-a is the writer of every
// shard and its router has applied the placements. The process stops when t ends.
func startA(t *testing.T, sys *system) *node {
	t.Helper()
	a, err := sys.start(t.Context(), nodeA)
	assert.NoError(t, err, "node-a starts")
	t.Cleanup(func() { <-a.Done() })
	assert.NoError(
		t,
		settle(t.Context(), sys.env.clock, sys.st, a.router, nodeA, slots),
		"every shard settles on node-a",
	)
	return a
}

// activate opens the tenure of the shard sid at epoch 1 on a bank of node-a over the storage of
// sys. The range of the tenure is the whole range, so the activation has every account. It
// returns the bank and the result of Open.
func activate(t *testing.T, sys *system) (*bank, replica.OpenResult) {
	t.Helper()
	tok := fence.Token{Shard: sid, Epoch: 1}
	assert.NoError(t, sys.storage.Acquire(t.Context(), tok), "the tenure acquires the fence")
	b := newBank(&bankConfig{storage: sys.storage, partitioner: sys.part, node: nodeA})
	res, err := b.Open(t.Context(), &replica.OpenRequest{
		Request: &protocol.Request{Shard: sid, Call: protocol.Add},
		Token:   tok,
	})
	assert.NoError(t, err, "the bank opens the tenure")
	return b, res
}
