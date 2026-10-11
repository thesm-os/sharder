// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Loss -linecomment -output=membership.string_gen.go

import (
	"context"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/tag"

	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The names in the records of the suite.
const (
	// namespace is the namespace of every scope of the suite.
	namespace shard.Namespace = "membershiptest"
	// cell is the cell of the scope that the nodes join.
	cell shard.CellID = "cell-1"
	// cpu is the load metric of the capacities of the nodes.
	cpu = "cpu"
	// zone is the label of the zone of a node.
	zone = "zone"
)

// The names of the actions of the machine of leases.
const (
	// actJoin joins a node that has no live session.
	actJoin = "join"
	// actLeave leaves the live session of a node.
	actLeave = "leave"
	// actStop ends the context of the join of a session that renews its lease.
	actStop = "stop"
	// actAdvance advances the clock of the leases.
	actAdvance = "advance"
)

// The labels of the events of the machine of leases, which each case of the machine counts.
const (
	// labelRejoined counts a case in which a node joins again after an earlier session.
	labelRejoined = "a node joins again"
	// labelLeft counts a case in which a node leaves.
	labelLeft = "a node leaves"
	// labelExpired counts a case in which the lease of a session expires.
	labelExpired = "a lease expires"
	// labelDeadline counts a case in which a session has lost its lease while the view
	// still lists its node.
	labelDeadline = "a session loses its lease before its expiry"
)

// The least shares of the cases of the machine of leases that count each label. Each event
// occurs in about three times as many cases of a correct membership, so a run decides each
// requirement after its first 100 cases.
const (
	// shareFrequent is the least share of the events that occur in about four cases in
	// five. A node that leaves and a node that joins again are such events.
	shareFrequent = 0.25
	// shareCommon is the least share of the events that occur in about half of the cases.
	// An expiry and a lease lost before its expiry are such events.
	shareCommon = 0.15
)

// The times of the suite.
const (
	// waitFor bounds each wait of the suite for a view, a closed Done or the end of a watch
	// that a correct membership delivers at once.
	waitFor = 10 * time.Second
	// maxAdvance is the longest step of the clock of the machine of leases, in whole
	// seconds.
	maxAdvance = 12
	// lostRenewals is the number of renewals of the laws of lost renewals before the local
	// deadline of the join, each of which a fault interrupts.
	lostRenewals = 2
	// beforeDeadline is the time after the join, with shortLease, of the last renewal before
	// the local deadline of the join at 8.5 seconds.
	beforeDeadline = 8 * time.Second
	// afterDeadline is the first whole second after the local deadline of the join.
	afterDeadline = 9 * time.Second
	// atExpiry is the expiry of the lease after the join, when no renewal applied.
	atExpiry = 10 * time.Second
	// atLateExpiry is the expiry of the lease after the renewal at beforeDeadline, when the
	// renewals applied and their replies were lost.
	atLateExpiry = 18 * time.Second
)

// lawTerm is the term of the commits that create the scopes of the laws of incarnations.
const lawTerm epoch.Epoch = 1

// tagIncarnation is the first byte of the namespace incarnations of the suite.
const tagIncarnation byte = 0x3e

// The identities, the nodes and the leases of the suite.
var (
	// incA is the namespace incarnation of the joins of the laws.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the joins of the laws of another incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// scope is the scope that the nodes join.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// origin is the start of the clock of each case.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
	// nodes lists the nodes of the suite, in the order of their IDs.
	nodes = [...]shard.Node{
		{
			ID:       "node-a",
			Cell:     cell,
			Address:  "node-a.membershiptest:7401",
			Identity: "spiffe://membershiptest/node-a",
			Capacity: map[string]float64{cpu: 4},
			Labels:   tag.Tags{{Key: zone, Value: "zone-a"}},
		},
		{
			ID:       "node-b",
			Cell:     cell,
			Address:  "node-b.membershiptest:7401",
			Identity: "spiffe://membershiptest/node-b",
			Capacity: map[string]float64{cpu: 8},
			Labels:   tag.Tags{{Key: zone, Value: "zone-b"}},
		},
		{
			ID:      "node-c",
			Cell:    cell,
			Address: "node-c.membershiptest:7401",
		},
	}
	// lease is the lease of the machine of leases. It renews every 3 seconds, with a
	// validity of 5 seconds and a TTL of 10 seconds. So a session that stops its renewals
	// loses its lease 5 seconds before the lease expires.
	lease = ownership.Lease{
		TTL:          shard.Duration(10 * time.Second),
		Renew:        shard.Duration(3 * time.Second),
		MaxDriftRate: 0.5,
		OnLoss:       ownership.Stop,
	}
	// shortLease is the lease of the laws of lost renewals. It renews at 4 and 8 seconds
	// after the join, with a validity of 8.5 seconds and a TTL of 10 seconds.
	shortLease = ownership.Lease{
		TTL:          shard.Duration(10 * time.Second),
		Renew:        shard.Duration(4 * time.Second),
		MaxDriftRate: 0.15,
		OnLoss:       ownership.Stop,
	}
	// advances generates the steps of the clock of the machine of leases, in whole seconds.
	advances = prop.Integer(1, maxAdvance).Map(func(s int) time.Duration { return time.Duration(s) * time.Second })
)

// Loss is the point at which a fault that [WithLostRenewals] injects interrupts a renewal of
// a lease.
//
// The zero Loss is not Valid.
//
// # Concurrency
//
// A Loss is a comparable value.
type Loss uint8

// The points at which a fault interrupts a renewal.
const (
	// LoseRequest loses a renewal before the backend applies it, so it moves neither the
	// expiry of the lease nor the local deadline of the session.
	LoseRequest Loss = 1 // request
	// LoseReply loses the reply of a renewal after the backend applies it, so it moves the
	// expiry of the lease and not the local deadline of the session.
	LoseReply Loss = 2 // reply
)

// Valid reports whether l is LoseRequest or LoseReply.
//
// # Allocation contract
//
// Zero alloc.
func (l Loss) Valid() bool {
	return l >= LoseRequest && l <= LoseReply
}

// config is the result of the Options of one run of AssertMembership.
type config struct {
	// lose makes the next renewal of a lease fail, or is nil.
	lose func(tb assert.TB, m membership.Membership, s store.Store, l Loss)
}

// Option configures a run of [AssertMembership]. Each Option adds the laws that its docblock
// lists.
type Option func(*config)

// WithLostRenewals gives the suite a way to make the next renewal of a lease of m fail at the
// point that l selects. s is the store that newMembership returned with m. The suite then
// checks that a session closes its Done at the local deadline after its join when every
// renewal or every reply of a renewal is lost, and that the backend keeps the node in the
// view until the expiry of the last renewal that it applied.
func WithLostRenewals(lose func(tb assert.TB, m membership.Membership, s store.Store, l Loss)) Option {
	return func(c *config) { c.lose = lose }
}

// cleanupTB is an assert.TB that runs a function when its test or case ends, as a
// *testing.T and a *prop.Case do.
type cleanupTB interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// sessionModel is the model of the session of one node in the machine of leases.
type sessionModel struct {
	// s is the session of the node, or nil when the node never joined.
	s membership.Session
	// cancel ends the context of the join.
	cancel context.CancelFunc
	// joined is the time of the join.
	joined time.Time
	// stopped is the time at which the context of the join ended, or zero while the session
	// renews its lease.
	stopped time.Time
	// inc is the incarnation of the session.
	inc epoch.Epoch
	// left reports that the node left.
	left bool
}

// at reports whether the lease of the session is live at now, and whether the session has
// lost its lease. The session renews its lease every Renew after the join, until the
// context of the join ends. The lease is live until TTL after its last renewal, and the
// session loses it at the validity of the lease after its last renewal, or when the node
// leaves.
func (sm *sessionModel) at(now time.Time) (live, lost bool) {
	if sm.s == nil || sm.left {
		return false, sm.left
	}
	end := now
	if !sm.stopped.IsZero() {
		end = sm.stopped
	}
	renewed := sm.joined.Add(end.Sub(sm.joined).Truncate(time.Duration(lease.Renew)))
	return now.Before(renewed.Add(time.Duration(lease.TTL))), !now.Before(renewed.Add(lease.Validity()))
}

// leases is the subject and the model of the machine of leases in one case: the membership,
// the clock of its leases, the time of the clock, and the session of each node.
type leases struct {
	// m is the membership of the case.
	m membership.Membership
	// clk is the clock of the leases.
	clk *fake.Clock
	// now is the time of clk.
	now time.Time
	// sessions lists the session of each node of nodes.
	sessions [len(nodes)]sessionModel
}

// actions returns the actions of the machine of leases: join a node without a live session,
// leave a live session, stop the renewals of a session, and advance the clock.
func (ls *leases) actions() []stateful.Action[struct{}] {
	idle := func(live, _ bool) bool { return !live }
	live := func(live, _ bool) bool { return live }
	renewing := func(_, renewing bool) bool { return renewing }
	return []stateful.Action[struct{}]{
		{
			Name:    actJoin,
			Enabled: func(struct{}) bool { return len(ls.pick(idle)) > 0 },
			Input:   func(c *prop.Case, _ struct{}) any { return c.Draw(prop.SampledFrom(ls.pick(idle)...), "node") },
			Run:     ls.join,
		},
		{
			Name:    actLeave,
			Enabled: func(struct{}) bool { return len(ls.pick(live)) > 0 },
			Input:   func(c *prop.Case, _ struct{}) any { return c.Draw(prop.SampledFrom(ls.pick(live)...), "node") },
			Run:     ls.leave,
		},
		{
			Name:    actStop,
			Enabled: func(struct{}) bool { return len(ls.pick(renewing)) > 0 },
			Input:   func(c *prop.Case, _ struct{}) any { return c.Draw(prop.SampledFrom(ls.pick(renewing)...), "node") },
			Run:     ls.stop,
		},
		{
			Name:  actAdvance,
			Input: func(c *prop.Case, _ struct{}) any { return c.Draw(advances, "advance") },
			Run:   ls.advance,
		},
	}
}

// pick returns the indexes of the nodes whose sessions keep reports true for, at the time of
// the machine. live reports that the lease of a session is live, and renewing that the
// session renews it.
func (ls *leases) pick(keep func(live, renewing bool) bool) []int {
	var picked []int
	for i := range ls.sessions {
		sm := &ls.sessions[i]
		live, _ := sm.at(ls.now)
		if keep(live, live && sm.stopped.IsZero()) {
			picked = append(picked, i)
		}
	}
	return picked
}

// join joins the node of the input with a context of its own, and records its session.
func (ls *leases) join(c *prop.Case, _ int, in any) {
	i, _ := in.(int)
	ctx, cancel := context.WithCancel(c.Context())
	c.Cleanup(cancel)
	n := nodes[i]
	ss, err := ls.m.Join(ctx, scope, incA, &n, lease)
	assert.NoError(c, err, "Join registers a node")
	if before := &ls.sessions[i]; before.s != nil {
		c.Classify(labelRejoined)
		assert.True(c, ss.Incarnation() > before.inc,
			"Join gives a node a session of a higher incarnation than its session before")
	}
	ls.sessions[i] = sessionModel{s: ss, cancel: cancel, joined: ls.now, inc: ss.Incarnation()}
}

// leave leaves the session of the node of the input.
func (ls *leases) leave(c *prop.Case, _ int, in any) {
	i, _ := in.(int)
	sm := &ls.sessions[i]
	c.Classify(labelLeft)
	assert.NoError(c, sm.s.Leave(c.Context()), "Leave returns nil")
	sm.left = true
	sm.cancel()
}

// stop ends the context of the join of the node of the input, which stops the renewals of
// its lease.
func (ls *leases) stop(_ *prop.Case, _ int, in any) {
	i, _ := in.(int)
	sm := &ls.sessions[i]
	sm.cancel()
	sm.stopped = ls.now
}

// advance advances the clock of the leases by the duration of the input.
func (ls *leases) advance(_ *prop.Case, _ int, in any) {
	d, _ := in.(time.Duration)
	ls.clk.Advance(d)
	ls.now = ls.now.Add(d)
}

// check fails c when the current view differs from the model, or when the Done of a session
// is closed while the session has its lease, or open after the session lost its lease.
func (ls *leases) check(c *prop.Case) {
	v := current(c, ls.m)
	var want []shard.Node
	for i := range ls.sessions {
		sm := &ls.sessions[i]
		live, lost := sm.at(ls.now)
		if live {
			n := nodes[i]
			n.Incarnation = sm.inc
			want = append(want, n)
		}
		if sm.s == nil {
			continue
		}
		if live && lost {
			c.Classify(labelDeadline)
		}
		if !live && !sm.left {
			c.Classify(labelExpired)
		}
		if lost {
			streamtest.Receive(c, sm.s.Done(), waitFor, "Done is closed once the session loses its lease")
			continue
		}
		streamtest.Blocks(c, sm.s.Done(), "Done is open while the session has its lease")
	}
	assert.Equal(c, v.Nodes, want,
		"the view lists the node of each session whose lease is live, in the order of their IDs", assert.EquateEmpty())
	assert.Equal(c, v.Scope, scope, "the view is a view of the scope of the watch")
	assert.Equal(c, v.Incarnation, incA, "the view is a view of the incarnation of the watch")
}

// settle fails c when the watch that sends to items did not yield every view up to the
// current view, in the order of their revisions.
func (ls *leases) settle(c *prop.Case, items <-chan streamtest.Item[membership.View]) {
	last := current(c, ls.m)
	var views []membership.View
	for range last.Revision + 1 {
		it, ok := streamtest.Receive(c, items, waitFor, "Watch yields each later view within the wait of the suite")
		assert.True(c, ok, "Watch yields each later view before its context ends")
		assert.NoError(c, it.Err, "Watch yields the views of the scope")
		views = append(views, it.Value)
		if it.Value.Revision >= last.Revision {
			break
		}
	}
	assert.Pairwise(c, views, func(earlier, later membership.View) bool { return earlier.Revision < later.Revision },
		"Watch yields the views in the order of their revisions")
	assert.Equal(c, views[len(views)-1], last, "the last view of a watch is the current view", assert.EquateEmpty())
}

// suite is one run of AssertMembership: the memberships that newMembership returns, and the
// options.
type suite struct {
	// newMembership returns an empty membership and the store over the same backend.
	newMembership func(tb assert.TB, clk clock.Clock) (membership.Membership, store.Store)
	// cfg is the result of the options.
	cfg config
}

// open returns a membership and its store that newMembership returns with leases on clk,
// and closes the store when tb ends.
func (su *suite) open(tb cleanupTB, clk clock.Clock) (membership.Membership, store.Store) {
	tb.Helper()
	m, s := su.newMembership(tb, clk)
	tb.Cleanup(func() { assert.NoError(tb, s.Close(), "Close returns nil") })
	return m, s
}

// joinLaws runs the laws of Join.
func (su *suite) joinLaws(t *testing.T) {
	t.Parallel()
	t.Run("registers each node until its lease expires", su.joinsUntilExpiry)
	t.Run("returns ErrIncarnation for a scope of another incarnation", su.joinsOtherIncarnation)
	t.Run("returns an error of the class Invalid for a node without an ID", su.joinsWithoutID)
	t.Run("returns an error of the class Invalid for a lease that the configuration refuses", su.joinsInvalidLease)
	t.Run("does not modify the node", su.joinsWithoutChange)
	t.Run("does not keep the node", su.joinsWithCopy)
}

// joinsUntilExpiry runs the machine of leases: joins, leaves, stopped renewals and steps of
// the clock, with the current view and the Done of each session checked after every step,
// and the views of one watch checked at the end.
func (su *suite) joinsUntilExpiry(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "the views of Watch list the node of each session whose lease is live", func(c *prop.Case) {
		clk := fake.New(origin)
		m, _ := su.open(c, clk)
		ls := &leases{m: m, clk: clk, now: origin}
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		items := streamtest.Follow(ctx, m.Watch(ctx, scope, incA))
		stateful.Steps(c, stateful.Machine[struct{}]{
			Actions:   ls.actions(),
			Invariant: func(c *prop.Case, _ struct{}) { ls.check(c) },
			Settle:    func(c *prop.Case, _ struct{}) { ls.settle(c, items) },
		}, stateful.Swarm(false))
	},
		prop.Require(labelLeft, shareFrequent),
		prop.Require(labelRejoined, shareFrequent),
		prop.Require(labelExpired, shareCommon),
		prop.Require(labelDeadline, shareCommon),
	)
}

// joinsOtherIncarnation checks that Join returns shard.ErrIncarnation for a scope that
// another incarnation created.
func (su *suite) joinsOtherIncarnation(t *testing.T) {
	t.Parallel()
	m, s := su.open(t, fake.New(origin))
	create(t, s)
	n := nodes[0]
	_, err := m.Join(t.Context(), scope, incB, &n, lease)
	assert.ErrorIs(t, err, shard.ErrIncarnation, "Join returns ErrIncarnation for a scope of another incarnation")
}

// joinsWithoutID checks that Join returns an error of the class Invalid for a node without
// an ID.
func (su *suite) joinsWithoutID(t *testing.T) {
	t.Parallel()
	m, _ := su.open(t, fake.New(origin))
	n := nodes[0]
	n.ID = ""
	_, err := m.Join(t.Context(), scope, incA, &n, lease)
	assert.Equal(
		t,
		errs.Classify(err),
		errs.Invalid,
		"Join returns an error of the class Invalid for a node without an ID",
	)
}

// joinsInvalidLease checks that Join returns an error of the class Invalid for a lease whose
// renewal interval is not below its validity.
func (su *suite) joinsInvalidLease(t *testing.T) {
	t.Parallel()
	m, _ := su.open(t, fake.New(origin))
	n := nodes[0]
	invalid := lease
	invalid.Renew = invalid.TTL
	_, err := m.Join(t.Context(), scope, incA, &n, invalid)
	assert.Equal(t, errs.Classify(err), errs.Invalid,
		"Join returns an error of the class Invalid for a lease that the configuration refuses")
}

// joinsWithoutChange checks that Join does not modify the node that it registers.
func (su *suite) joinsWithoutChange(t *testing.T) {
	t.Parallel()
	m, _ := su.open(t, fake.New(origin))
	n := nodes[0].CloneKanon()
	assert.Pure(t, func() *shard.Node { return n.CloneKanon() }, func() {
		_, err := m.Join(t.Context(), scope, incA, n, lease)
		assert.NoError(t, err, "Join registers a node")
	}, "Join does not modify the node that it registers")
}

// joinsWithCopy checks that a view does not change when the caller changes the node of a
// join after the join returned.
func (su *suite) joinsWithCopy(t *testing.T) {
	t.Parallel()
	m, _ := su.open(t, fake.New(origin))
	n := nodes[0].CloneKanon()
	ss, err := m.Join(t.Context(), scope, incA, n, lease)
	assert.NoError(t, err, "Join registers a node")
	want := nodes[0]
	want.Incarnation = ss.Incarnation()
	n.Address = nodes[1].Address
	n.Capacity[cpu] = nodes[1].Capacity[cpu]
	n.Labels[0] = nodes[1].Labels[0]
	assert.Equal(t, current(t, m).Nodes, []shard.Node{want},
		"a view does not change when the caller changes the node of a join")
}

// watchLaws runs the laws of Watch apart from the machine of leases.
func (su *suite) watchLaws(t *testing.T) {
	t.Parallel()
	t.Run("yields ErrIncarnation for a scope of another incarnation", su.watchesOtherIncarnation)
	t.Run("yields ErrIncarnation when another incarnation creates the scope", su.watchesCreation)
	t.Run("ends once its context ends", su.watchesUntilCancel)
}

// watchesOtherIncarnation checks that a watch of a scope of another incarnation yields
// shard.ErrIncarnation and ends.
func (su *suite) watchesOtherIncarnation(t *testing.T) {
	t.Parallel()
	m, s := su.open(t, fake.New(origin))
	create(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, m.Watch(ctx, scope, incB))
	it, ok := streamtest.Receive(t, items, waitFor, "Watch yields an error within the wait of the suite")
	assert.True(t, ok, "Watch yields an error before it ends")
	assert.ErrorIs(t, it.Err, shard.ErrIncarnation, "Watch yields ErrIncarnation for a scope of another incarnation")
	streamtest.End(t, items, waitFor, "Watch ends after ErrIncarnation")
}

// watchesCreation checks that a watch of a scope before its first commit yields
// shard.ErrIncarnation and ends when another incarnation creates the scope.
func (su *suite) watchesCreation(t *testing.T) {
	t.Parallel()
	m, s := su.open(t, fake.New(origin))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, m.Watch(ctx, scope, incB))
	it, ok := streamtest.Receive(t, items, waitFor, "Watch yields the current view within the wait of the suite")
	assert.True(t, ok, "Watch yields the current view of a scope before its first commit")
	assert.NoError(t, it.Err, "Watch yields the current view of a scope before its first commit")
	create(t, s)
	it, ok = streamtest.Receive(t, items, waitFor, "Watch yields an error within the wait of the suite")
	assert.True(t, ok, "Watch yields an error before it ends")
	assert.ErrorIs(
		t,
		it.Err,
		shard.ErrIncarnation,
		"Watch yields ErrIncarnation when another incarnation creates the scope",
	)
	streamtest.End(t, items, waitFor, "Watch ends after ErrIncarnation")
}

// watchesUntilCancel checks that a watch ends once its context ends.
func (su *suite) watchesUntilCancel(t *testing.T) {
	t.Parallel()
	m, _ := su.open(t, fake.New(origin))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, m.Watch(ctx, scope, incA))
	_, ok := streamtest.Receive(t, items, waitFor, "Watch yields the current view within the wait of the suite")
	assert.True(t, ok, "Watch yields the current view")
	cancel()
	streamtest.End(t, items, waitFor, "Watch ends once its context ends")
}

// sessionLaws runs the laws of the sessions that WithLostRenewals adds.
func (su *suite) sessionLaws(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		loss Loss
		// listed is the number of nodes that the view lists at the expiry of the join.
		listed int
	}{
		{name: "closes Done at the deadline of the join when each renewal is lost", loss: LoseRequest},
		{
			name:   "closes Done at the deadline of the join when the reply of each renewal is lost",
			loss:   LoseReply,
			listed: 1,
		},
	}
	if su.cfg.lose != nil {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				su.losesRenewals(t, tt.loss, tt.listed)
			})
		}
	}
}

// losesRenewals checks that a session whose renewals the fault l interrupts closes its Done
// at the deadline of its join, and that the view lists its node until the expiry of the last
// renewal that applied. listed is the number of nodes that the view lists at the expiry of
// the join: one when the renewals applied, and none otherwise.
func (su *suite) losesRenewals(t *testing.T, l Loss, listed int) {
	t.Helper()
	clk := fake.New(origin)
	m, s := su.open(t, clk)
	n := nodes[0]
	ss, err := m.Join(t.Context(), scope, incA, &n, shortLease)
	assert.NoError(t, err, "Join registers a node")
	for range lostRenewals {
		su.cfg.lose(t, m, s, l)
	}
	clk.Advance(beforeDeadline)
	assert.Length(t, current(t, m).Nodes, 1, "the view lists the node before its deadline")
	streamtest.Blocks(t, ss.Done(), "Done is open before the deadline of the join")
	clk.Advance(afterDeadline - beforeDeadline)
	assert.Length(t, current(t, m).Nodes, 1, "the view lists the node until the expiry of its lease")
	streamtest.Receive(t, ss.Done(), waitFor, "Done is closed at the deadline of the join when no renewal is accepted")
	clk.Advance(atExpiry - afterDeadline)
	assert.Length(t, current(t, m).Nodes, listed,
		"the view lists the node at the expiry of the join exactly when the renewals applied")
	clk.Advance(atLateExpiry - atExpiry)
	assert.Empty(t, current(t, m).Nodes, "the view lists the node until the expiry of the last renewal that applied")
}

// AssertMembership checks the laws of package membership against the memberships that
// newMembership returns. newMembership returns an empty membership whose leases run on
// clk, and the store over the same backend, and fails tb when it cannot open them. Each case
// of a law opens a membership of its own on a clock of its own, and the suite closes the
// store when its case ends. A case can call newMembership on a goroutine other than the
// goroutine of the test, with an assert.TB that is not a *testing.T.
//
// The suite runs one subtest per method of the membership:
//
//   - Join registers each node until its lease expires. A machine joins, leaves and stops
//     the renewals of nodes and advances the clock. After every step it checks that the
//     current view lists the node of each session whose lease is live, and that the Done
//     of a session is closed exactly when the session lost its lease. Join returns
//     shard.ErrIncarnation for a scope of another incarnation. It returns an error of the
//     class Invalid for a node without an ID, and for a lease that the configuration
//     refuses. It does not modify the node, and does not keep it.
//   - Watch yields every view in the order of their revisions, ends with its context, and
//     yields shard.ErrIncarnation for a scope of another incarnation.
//
// Each [Option] adds the laws that its docblock lists.
//
// # Concurrency
//
// The subtests of the suite run in parallel.
func AssertMembership(
	t *testing.T,
	newMembership func(tb assert.TB, clk clock.Clock) (membership.Membership, store.Store),
	opts ...Option,
) {
	t.Helper()
	su := &suite{newMembership: newMembership}
	for _, o := range opts {
		o(&su.cfg)
	}
	t.Run("Join", su.joinLaws)
	t.Run("Watch", su.watchLaws)
	t.Run("Session", su.sessionLaws)
}

// create creates the scope through a commit of the incarnation A.
func create(tb assert.TB, s store.Store) {
	tb.Helper()
	m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1}}
	_, err := s.Writer(scope, incA, lawTerm).Commit(assert.Context(tb), 0, &m)
	assert.NoError(tb, err, "Commit creates the scope")
}

// current returns the current view of the scope that m keeps for the incarnation A: the
// first view of a new watch.
func current(tb assert.TB, m membership.Membership) membership.View {
	tb.Helper()
	ctx, cancel := context.WithCancel(assert.Context(tb))
	defer cancel()
	it, ok := streamtest.Receive(tb, streamtest.Follow(ctx, m.Watch(ctx, scope, incA)), waitFor,
		"Watch yields the current view within the wait of the suite")
	assert.True(tb, ok, "Watch yields the current view before it ends")
	assert.NoError(tb, it.Err, "Watch yields the views of the scope")
	return it.Value
}
