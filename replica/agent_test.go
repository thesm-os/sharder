// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package replica_test

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The weights of the actions of the property of an agent that a case takes more often than
// the others.
const (
	// adds is the weight of an add.
	adds = 3
	// drops is the weight of a drop.
	drops = 2
	// replays is the weight of a replayed callback.
	replays = 2
)

// The labels of the cases of the property of an agent, which its coverage requirements
// count.
const (
	// labelDrop counts a case that drops a tenure.
	labelDrop = "a tenure is dropped"
	// labelReplay counts a case that replays a callback.
	labelReplay = "a callback is replayed"
	// labelStale counts a case that passes a callback below the epoch of its shard.
	labelStale = "a callback is stale"
	// labelRejoin counts a case in which the node joins its cell again.
	labelRejoin = "the node joins its cell again"
	// labelEarlier counts a case that passes a callback of an earlier incarnation.
	labelEarlier = "a callback is of an earlier incarnation"
)

// The least shares of the cases of the property of an agent that the labels must count. Over
// 1,000 cases, a drop, a replay, a stale callback and a callback of an earlier incarnation
// each counted 17% to 23% of the cases, and a rejoin counted 45%. Each least share is about
// half of the measured share.
const (
	// common is the least share of a drop, a replay, a stale callback and a callback of an
	// earlier incarnation.
	common = 0.08
	// rejoins is the least share of a rejoin.
	rejoins = 0.2
)

// errStop is the cause with which a test ends the context of Start.
var errStop = errors.New("test: the process stops")

// incB is a namespace incarnation other than the incarnation of the tests.
var incB = shard.NamespaceIncarnation{ID: id.New128([id.Size128]byte{0: 0x1b, 15: 0x0b})}

// allShards lists the shards of the property of an agent.
var allShards = []shard.ID{shardA, shardB}

// intruder is the metadata of a callback of a caller other than the controller of the cell.
var intruder = protocol.Metadata{Caller: "intruder", Version: protocol.Current}

// plain is a membership that does not provide a capability. Its method set has the methods
// of membership.Membership alone.
type plain struct {
	membership.Membership
}

// gated is a store whose watches return only once waitFor has passed since newGated built
// it. It embeds a memory.Store, so it provides the capabilities of that store.
type gated struct {
	*memory.Store
	// gate is closed once waitFor has passed.
	gate chan struct{}
	// opened is true once gate is closed.
	opened atomic.Bool
}

// newGated returns a store over st whose watches return only once waitFor has passed.
func newGated(st *memory.Store) *gated {
	g := &gated{Store: st, gate: make(chan struct{})}
	time.AfterFunc(waitFor, func() {
		g.opened.Store(true)
		close(g.gate)
	})
	return g
}

// Watch yields the items of the watch of the memory store. Once that watch has ended, Watch
// returns only after gate is closed.
func (g *gated) Watch(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, from epoch.Epoch,
	n shard.NodeID,
) iter.Seq2[store.Event, error] {
	return func(yield func(store.Event, error) bool) {
		for ev, err := range g.Store.Watch(ctx, s, inc, from, n) {
			if !yield(ev, err) {
				break
			}
		}
		<-g.gate
	}
}

// tenure is what the model of an agent expects of one shard on the node.
type tenure struct {
	// epoch is the epoch of the active tenure of the node, and zero without one.
	epoch epoch.Epoch
	// placed is the highest epoch at which the test placed the shard.
	placed epoch.Epoch
	// seen is the highest epoch of a callback of the shard that the agent ran.
	seen epoch.Epoch
	// op is the sequence of the newest operation of the shard.
	op uint64
}

// model is the state that the property of an agent expects of the agent. It has the tenure
// of each shard and the incarnations of the node. It also lists the callbacks that the agent
// ran and the keys of the callbacks that the agent refused.
type model struct {
	// f is the fixture of the agent.
	f *fixture
	// a is the agent under test.
	a *replica.Agent
	// shards maps each shard of allShards to its tenure.
	shards map[shard.ID]*tenure
	// refused contains the key of each callback that the agent refused.
	refused map[protocol.Key]bool
	// ran lists each callback that the agent ran, in order.
	ran []*protocol.Request
	// incarnation is the incarnation of the session of the node.
	incarnation epoch.Epoch
	// previous is the incarnation of the session before the current one, and zero before the
	// first rejoin.
	previous epoch.Epoch
}

// newModel returns the model of an agent that serves, whose node has no tenure.
func newModel(c *prop.Case) *model {
	f := newFixture(c)
	m := &model{
		f:       f,
		a:       f.start(c),
		shards:  make(map[shard.ID]*tenure, len(allShards)),
		refused: make(map[protocol.Key]bool),
	}
	for _, sid := range allShards {
		m.shards[sid] = &tenure{}
	}
	m.incarnation = f.incarnation(c)
	return m
}

// do passes r to the agent as a callback of the controller of the cell. A callback that does
// not end within waitFor gets the cause of its context, so a step that waits forever fails
// its case.
func (m *model) do(c *prop.Case, r *protocol.Request) error {
	ctx, cancel := context.WithTimeout(c.Context(), waitFor)
	defer cancel()
	return m.a.Do(ctx, controllerMetadata, r)
}

// add places the shard sid at its next epoch, and passes the add of its next operation.
func (m *model) add(c *prop.Case, sid shard.ID) {
	t := m.shards[sid]
	e, op := t.placed+1, t.op+1
	m.f.place(c, sid, e)
	r := request(protocol.Add, sid, op, e, m.incarnation)
	assert.NoError(c, m.do(c, r), "an add of a placed tenure must succeed")
	m.ran = append(m.ran, r)
	t.epoch, t.placed, t.seen, t.op = e, e, e, op
}

// drop passes the drop of the active tenure of the shard sid in its next operation, and
// then places the shard on another node.
func (m *model) drop(c *prop.Case, sid shard.ID) {
	c.Classify(labelDrop)
	t := m.shards[sid]
	op := t.op + 1
	r := request(protocol.Drop, sid, op, t.epoch, m.incarnation)
	assert.NoError(c, m.do(c, r), "a drop of the active tenure must succeed")
	m.ran = append(m.ran, r)
	m.f.unplace(c, sid, t.placed+1)
	t.epoch, t.placed, t.op = 0, t.placed+1, op
}

// replay passes the callback i of ran again. A callback of the newest operation of its shard
// at the incarnation of the node gets its kept result. The agent refuses every other one as
// stale.
func (m *model) replay(c *prop.Case, i int) {
	c.Classify(labelReplay)
	r := m.ran[i]
	err := m.do(c, r)
	if r.NodeIncarnation == m.incarnation && r.Operation.Sequence == m.shards[r.Shard].op {
		assert.NoError(c, err, "a replayed callback of the newest operation must get its kept result")
		return
	}
	assert.Equal(c, errs.Classify(err), errs.Conflict, "a replayed callback of an older operation must be stale")
}

// stale passes a new operation of the shard sid at an epoch below the highest epoch of a
// callback that the agent ran. The agent must refuse it before the handler.
func (m *model) stale(c *prop.Case, sid shard.ID) {
	c.Classify(labelStale)
	t := m.shards[sid]
	r := request(protocol.Add, sid, t.op+1, t.seen-1, m.incarnation)
	m.refused[r.Key()] = true
	assert.Equal(c, errs.Classify(m.do(c, r)), errs.Conflict, "a callback below the epoch of the shard must be stale")
}

// earlier passes a new operation of the shard sid for the previous incarnation of the node.
// The agent must refuse it before the handler.
func (m *model) earlier(c *prop.Case, sid shard.ID) {
	c.Classify(labelEarlier)
	t := m.shards[sid]
	r := request(protocol.Add, sid, t.op+1, t.placed+1, m.previous)
	m.refused[r.Key()] = true
	assert.Equal(c, errs.Classify(m.do(c, r)), errs.Conflict,
		"a callback of an earlier incarnation of the node must be stale")
}

// rejoin ends the session of the node, and waits until the agent has joined the cell again.
func (m *model) rejoin(c *prop.Case) {
	c.Classify(labelRejoin)
	m.previous = m.incarnation
	m.incarnation = m.f.rejoin(c)
}

// unbounded passes a Close without a deadline. The agent must refuse it and run on.
func (m *model) unbounded(c *prop.Case) {
	assert.ErrorIs(c, m.a.Close(c.Context()), replica.ErrConfig, "a Close without a deadline must be refused")
	assert.Nil(c, m.a.Err(), "a refused Close must not end the run of the agent")
}

// invariant checks the handler and the tokens after every step: the handler ran each key
// once and no key of a refused callback, and Token returns a token only for the active
// tenure of a shard.
func (m *model) invariant(c *prop.Case) {
	counts := m.f.app.counts()
	for k, n := range counts {
		expect.Equal(c, n, 1, "the handler must run each key of a callback once")
		expect.False(c, m.refused[k], "the handler must not run a callback that the agent refused")
	}
	for _, sid := range allShards {
		res, err := m.a.Token(c.Context(), &replica.TokenRequest{Shard: sid})
		if err != nil {
			continue
		}
		expect.Equal(c, res.Token.Epoch, m.shards[sid].epoch, "Token must return only the token of the active tenure")
	}
}

// exercise runs the steps of the case c over an agent that serves, as its controller and its
// application drive it. The steps add and drop tenures, replay callbacks, pass stale
// callbacks and callbacks of an earlier incarnation, end the session of the node, and pass
// a Close without a deadline. The model checks after every step that the handler runs each
// key once and never a refused callback, and that Token returns only an active token.
func exercise(c *prop.Case) {
	m := newModel(c)
	pick := func(c *prop.Case, _ struct{}) any {
		return allShards[c.Draw(prop.Integer(0, len(allShards)-1), "shard")]
	}
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: []stateful.Action[struct{}]{
			{
				Name: "add", Weight: adds, Input: pick,
				Run: func(c *prop.Case, _ int, in any) { m.add(c, in.(shard.ID)) },
			},
			{
				Name: "drop", Weight: drops, Input: pick,
				Enabled: func(struct{}) bool {
					for _, t := range m.shards {
						if t.epoch != 0 {
							return true
						}
					}
					return false
				},
				Run: func(c *prop.Case, _ int, in any) {
					if sid := in.(shard.ID); m.shards[sid].epoch != 0 {
						m.drop(c, sid)
					}
				},
			},
			{
				Name:    "replay",
				Weight:  replays,
				Enabled: func(struct{}) bool { return len(m.ran) > 0 },
				Input:   func(c *prop.Case, _ struct{}) any { return c.Draw(prop.Integer(0, len(m.ran)-1), "callback") },
				Run:     func(c *prop.Case, _ int, in any) { m.replay(c, in.(int)) },
			},
			{
				Name: "stale", Input: pick,
				Run: func(c *prop.Case, _ int, in any) {
					if sid := in.(shard.ID); m.shards[sid].seen >= 2 {
						m.stale(c, sid)
					}
				},
			},
			{
				Name: "earlier", Input: pick,
				Enabled: func(struct{}) bool { return m.previous != 0 },
				Run:     func(c *prop.Case, _ int, in any) { m.earlier(c, in.(shard.ID)) },
			},
			{Name: "rejoin", Run: func(c *prop.Case, _ int, _ any) { m.rejoin(c) }},
			{Name: "close without a deadline", Run: func(c *prop.Case, _ int, _ any) { m.unbounded(c) }},
		},
		Invariant: func(c *prop.Case, _ struct{}) { m.invariant(c) },
	})
}

// TestAgent checks the start of an agent, the callbacks that it serves, its Close and the end
// of its run.
func TestAgent(t *testing.T) {
	t.Parallel()

	t.Run("Start", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a nil Config", func(t *testing.T) {
			t.Parallel()
			_, err := replica.Start(t.Context(), nil, newApplication())
			assert.ErrorIs(t, err, replica.ErrConfig, "Start must refuse a nil Config")
		})

		tests := []struct {
			give func(c *replica.Config)
			name string
		}{
			{name: "returns ErrConfig for a Config without a store", give: func(c *replica.Config) { c.Store = nil }},
			{
				name: "returns ErrConfig for a Config without a membership",
				give: func(c *replica.Config) { c.Membership = nil },
			},
			{
				name: "returns ErrConfig for a Config without a controller client",
				give: func(c *replica.Config) { c.Controller = nil },
			},
			{
				name: "returns ErrConfig for a Config without a fence guard",
				give: func(c *replica.Config) { c.Guard = nil },
			},
			{name: "returns ErrConfig for a Config without a clock", give: func(c *replica.Config) { c.Clock = nil }},
			{
				name: "returns ErrConfig for a Config without a source of randomness",
				give: func(c *replica.Config) { c.Rand = nil },
			},
			{
				name: "returns ErrConfig for a Config without a reporter",
				give: func(c *replica.Config) { c.Reporter = nil },
			},
			{
				name: "returns ErrConfig for a Config without a registry",
				give: func(c *replica.Config) { c.Registry = nil },
			},
			{name: "returns ErrConfig for a Config without a logger", give: func(c *replica.Config) { c.Logger = nil }},
			{
				name: "returns ErrConfig for a Config without a namespace",
				give: func(c *replica.Config) { c.Namespace = "" },
			},
			{
				name: "returns ErrConfig for a Config without the ID of the node",
				give: func(c *replica.Config) { c.Node.ID = "" },
			},
			{
				name: "returns ErrConfig for a Config without the address of the node",
				give: func(c *replica.Config) { c.Node.Address = "" },
			},
			{
				name: "returns ErrConfig for a node whose cell is not the text of a shard ID",
				give: func(c *replica.Config) { c.Node.Cell = "other" },
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				f := bare(t)
				cfg := f.config()
				tt.give(cfg)
				_, err := replica.Start(t.Context(), cfg, f.app)
				assert.ErrorIs(t, err, replica.ErrConfig, "Start must refuse the Config")
			})
		}

		t.Run("returns ErrConfig for a nil handler", func(t *testing.T) {
			t.Parallel()
			_, err := replica.Start(t.Context(), bare(t).config(), nil)
			assert.ErrorIs(t, err, replica.ErrConfig, "Start must refuse a nil handler")
		})

		t.Run("returns an agent once the node has a session in its cell", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.start(t)
				v := f.view(t)
				_, ok := v.Node(node.ID)
				assert.True(t, ok, "the node must have a session in its cell once Start returns")
			})
		})

		t.Run("returns an error of class errs.Invalid for a cell that the namespace does not have",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					cfg := f.config()
					cfg.Node.Cell = shard.CellID(shardA.String())
					_, err := replica.Start(t.Context(), cfg, f.app)
					assert.Equal(
						t,
						errs.Classify(err),
						errs.Invalid,
						"Start must refuse a cell that the namespace does not have",
					)
				})
			})

		t.Run("returns an error of class errs.Invalid for a membership without the capabilities that the spec requires",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					cfg := f.config()
					cfg.Membership = plain{Membership: cfg.Membership}
					_, err := replica.Start(t.Context(), cfg, f.app)
					assert.Equal(t, errs.Classify(err), errs.Invalid,
						"Start must refuse a membership that does not provide the leases of the spec")
				})
			})

		t.Run("returns the cause of its context when the context ends before the namespace exists",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := bare(t)
					ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, errStop)
					defer cancel()
					_, err := replica.Start(ctx, f.config(), f.app)
					assert.ErrorIs(t, err, errStop, "Start must return the cause of its context")
				})
			})

		t.Run("returns the error of an ended context once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := bare(t)
				g := newGated(f.st)
				cfg := f.config()
				cfg.Store = g
				ctx, cancel := context.WithTimeoutCause(t.Context(), time.Minute, errStop)
				defer cancel()
				_, err := replica.Start(ctx, cfg, f.app)
				assert.ErrorIs(t, err, errStop, "Start must return the cause of its context")
				assert.True(t, g.opened.Load(), "Start must return once the watches of the agent have returned")
			})
		})
	})

	t.Run("Do", func(t *testing.T) {
		t.Parallel()

		t.Run("runs an add of a placed tenure through the handler", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.place(t, shardA, 1)
				r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the add must succeed")
				assert.Equal(t, f.app.called(r), 1, "the handler must run the add")
			})
		})

		t.Run("returns the kept result of a repeated key without the handler", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.place(t, shardA, 1)
				r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the add must succeed")
				assert.NoError(t, a.Do(t.Context(), controllerMetadata, r), "the repeated add must get the kept result")
				assert.Equal(t, f.app.called(r), 1, "the handler must run the key once")
			})
		})

		t.Run("returns an error of class errs.Conflict for a callback of an earlier incarnation of the node",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					a := f.start(t)
					f.place(t, shardA, 1)
					r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
					f.rejoin(t)
					assert.Equal(t, errs.Classify(a.Do(t.Context(), controllerMetadata, r)), errs.Conflict,
						"the agent must refuse a callback of an ended incarnation")
					assert.Equal(t, f.app.called(r), 0, "the handler must not run a refused callback")
				})
			})

		t.Run("returns an error of class errs.Conflict for a callback below the epoch of the shard",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					a := f.start(t)
					inc := f.incarnation(t)
					f.place(t, shardA, 2)
					assert.NoError(t, a.Do(t.Context(), controllerMetadata, request(protocol.Add, shardA, 1, 2, inc)),
						"the add must succeed")
					r := request(protocol.Add, shardA, 2, 1, inc)
					assert.Equal(t, errs.Classify(a.Do(t.Context(), controllerMetadata, r)), errs.Conflict,
						"the agent must refuse a callback below the epoch of the shard")
					assert.Equal(t, f.app.called(r), 0, "the handler must not run a refused callback")
				})
			})

		t.Run("returns an error of class errs.Denied for a caller other than the controller of the cell",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					a := f.start(t)
					f.place(t, shardA, 1)
					r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
					assert.Equal(t, errs.Classify(a.Do(t.Context(), intruder, r)), errs.Denied,
						"the agent must refuse a caller other than the controller of the cell")
					assert.Equal(t, f.app.called(r), 0, "the handler must not run a refused callback")
				})
			})

		t.Run("runs a callback of a caller other than the controller when Config.Unauthenticated is true",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t)
					a := f.start(t, func(c *replica.Config) { c.Unauthenticated = true })
					f.place(t, shardA, 1)
					r := request(protocol.Add, shardA, 1, 1, f.incarnation(t))
					assert.NoError(t, a.Do(t.Context(), intruder, r), "an unauthenticated agent must run the callback")
					assert.Equal(t, f.app.called(r), 1, "the handler must run the callback")
				})
			})

		t.Run("returns ErrNotServing once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancel(t.Context())
				a, err := replica.Start(ctx, f.config(), f.app)
				assert.NoError(t, err, "Start must start the agent")
				stop()
				received(t, a.Done(), "the end of the run")
				err = a.Do(t.Context(), controllerMetadata, request(protocol.Add, shardA, 1, 1, 1))
				expect.That(t, err).ErrorIs(replica.ErrNotServing, "a callback after the run must not be served")
				expect.Equal(t, errs.Classify(err), errs.Transient, "a callback after the run must be transient")
			})
		})

		t.Run("keeps one effect of the handler for each key under replays, stale callbacks and new incarnations",
			func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					prop.ForAll(t, "the handler runs each key of a callback once and no callback that the agent "+
						"refuses, and Token returns a token only for the active tenure of a shard", exercise,
						prop.Require(labelDrop, common), prop.Require(labelReplay, common),
						prop.Require(labelStale, common), prop.Require(labelEarlier, common),
						prop.Require(labelRejoin, rejoins))
				})
			})
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		t.Run("returns ErrConfig for a context without a deadline", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				assert.ErrorIs(
					t,
					a.Close(t.Context()),
					replica.ErrConfig,
					"Close must refuse a context without a deadline",
				)
				assert.Nil(t, a.Err(), "the agent must run on after a refused Close")
			})
		})

		t.Run("leaves the cell once the controller admits its disruption", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				ctx, cancel := f.closing(t, errStop)
				defer cancel()
				assert.NoError(t, a.Close(ctx), "Close must end once the controller admits it")
				v := f.view(t)
				_, ok := v.Node(node.ID)
				expect.False(t, ok, "the node must have left its cell")
				expect.Length(t, f.ctrl.dones, 1, "the agent must end the reservation of its disruption")
			})
		})

		t.Run("returns once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				g := newGated(f.st)
				a := f.start(t, func(c *replica.Config) { c.Store = g })
				ctx, cancel := f.closing(t, errStop)
				defer cancel()
				assert.NoError(t, a.Close(ctx), "Close must end once the controller admits it")
				assert.True(t, g.opened.Load(), "Close must return once the watches of the agent have returned")
			})
		})

		t.Run("returns the cause of its context when the controller defers the disruption", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				f.ctrl.decide = func(*budget.Disruption) budget.Decision { return budget.Decision{RetryAfter: time.Minute} }
				a := f.start(t)
				ctx, cancel := f.closing(t, errStop)
				defer cancel()
				assert.ErrorIs(t, a.Close(ctx), errStop, "Close must return the cause of its context")
				expect.Empty(t, f.ctrl.dones, "the agent must not end a reservation that the controller did not admit")
				expect.Nil(t, a.Err(), "Err must be nil after a Close")
			})
		})

		t.Run("returns ErrNotServing once the run of the agent has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancel(t.Context())
				a, err := replica.Start(ctx, f.config(), f.app)
				assert.NoError(t, err, "Start must start the agent")
				stop()
				received(t, a.Done(), "the end of the run")
				closing, cancel := f.closing(t, errStop)
				defer cancel()
				assert.ErrorIs(t, a.Close(closing), replica.ErrNotServing, "a Close after the run must not be served")
			})
		})
	})

	t.Run("Done", func(t *testing.T) {
		t.Parallel()

		t.Run("closes once the context of Start ends", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancel(t.Context())
				a, err := replica.Start(ctx, f.config(), f.app)
				assert.NoError(t, err, "Start must start the agent")
				stop()
				received(t, a.Done(), "the end of the run")
			})
		})
	})

	t.Run("Err", func(t *testing.T) {
		t.Parallel()

		t.Run("returns nil while the agent runs", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				assert.Nil(t, newFixture(t).start(t).Err(), "Err must be nil while the agent runs")
			})
		})

		t.Run("returns the cause of the context of Start once the run has ended", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				ctx, stop := context.WithCancelCause(t.Context())
				a, err := replica.Start(ctx, f.config(), f.app)
				assert.NoError(t, err, "Start must start the agent")
				stop(errStop)
				received(t, a.Done(), "the end of the run")
				assert.ErrorIs(t, a.Err(), errStop, "Err must return the cause of the context of Start")
			})
		})

		t.Run("returns nil after a Close that ended the run", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				ctx, cancel := f.closing(t, errStop)
				defer cancel()
				assert.NoError(t, a.Close(ctx), "Close must end once the controller admits it")
				assert.Nil(t, a.Err(), "Err must be nil after a Close")
			})
		})

		t.Run("returns the error of the agent once the root creates the namespace again", func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				a := f.start(t)
				f.st.Delete(cellScope)
				m := store.Mutation{ID: store.CommitID{Incarnation: incB, Term: cellTerm, Seq: 1}}
				_, err := f.st.Writer(cellScope, incB, cellTerm).Commit(t.Context(), 0, &m)
				assert.NoError(t, err, "the controller of the namespace that the root creates again must commit")
				received(t, a.Done(), "the end of the run")
				assert.ErrorIs(t, a.Err(), shard.ErrIncarnation, "Err must return the error of the agent")
			})
		})
	})
}
