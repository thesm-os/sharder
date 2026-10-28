// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package membershiptest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"sync"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
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
	"go.thesmos.sh/sharder/store/memory"
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
	// anonymous is the ID that the planted defect of JoinsRefuseNodesWithoutAnID gives a
	// node without an ID.
	anonymous shard.NodeID = "anonymous"
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
	// lostRenewals is the number of renewals of the checks of lost renewals before the
	// local deadline of the join, each of which a fault interrupts.
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

// lawTerm is the term of the commits that create the scopes of the checks of
// incarnations.
const lawTerm epoch.Epoch = 1

// tagIncarnation is the first byte of the namespace incarnations of the suite.
const tagIncarnation byte = 0x3e

// The identities, the nodes and the leases of the suite.
var (
	// incA is the namespace incarnation of the joins of the checks.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the joins of the checks of another incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// scope is the scope that the nodes join.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// origin is the start of the time of the machine of leases, and of the clock of the
	// memory stores of the planted defects.
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
	// shortLease is the lease of the checks of lost renewals. It renews at 4 and 8 seconds
	// after the join, with a validity of 8.5 seconds and a TTL of 10 seconds.
	shortLease = ownership.Lease{
		TTL:          shard.Duration(10 * time.Second),
		Renew:        shard.Duration(4 * time.Second),
		MaxDriftRate: 0.15,
		OnLoss:       ownership.Stop,
	}
	// advances generates the steps of the clock of the machine of leases, in whole seconds.
	advances = prop.Integer(1, maxAdvance).Map(func(s int) time.Duration { return time.Duration(s) * time.Second })
	// lossPoints maps each loss to the point of a memory store that loses a renewal there.
	lossPoints = map[Loss]memory.Point{LoseRequest: memory.PointRenewApply, LoseReply: memory.PointRenewAck}
)

// Loss is the point at which a fault that [Faulty.LoseRenewals] injects interrupts a renewal
// of a lease.
//
// [Loss.IsValid] refuses the zero Loss.
//
// # Concurrency
//
// A Loss is a comparable value.
//
//dokimi:enum text=comment
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

// Backed is a membership over a store whose leases run on a clock that the checks advance.
// The checks of incarnations create a scope through its store, and the checks of leases
// advance its clock.
type Backed interface {
	// Store returns the store over the backend of the membership.
	Store() store.Store
	// Advance moves the clock of the leases of the membership forward by d, and applies
	// every renewal, deadline and expiry that is due by then.
	Advance(d time.Duration)
}

// Faulty is a membership whose next renewals a fault interrupts. The checks of lost
// renewals call it.
type Faulty interface {
	// LoseRenewals makes the next renewal of a lease of the membership fail at the point
	// that l selects.
	LoseRenewals(l Loss)
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
// its clock, the time of the model, and the session of each node.
type leases struct {
	// h is the handle of the case.
	h *MembershipHandle
	// m is the membership of the case.
	m membership.Membership
	// backed advances the clock of the leases of m.
	backed Backed
	// now is the time of the model, which moves with the clock of m.
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
	assert.NoError(c, err, ls.h.Statement())
	if before := &ls.sessions[i]; before.s != nil {
		c.Classify(labelRejoined)
		assert.True(c, ss.Incarnation() > before.inc, ls.h.Statement())
	}
	ls.sessions[i] = sessionModel{s: ss, cancel: cancel, joined: ls.now, inc: ss.Incarnation()}
}

// leave leaves the session of the node of the input.
func (ls *leases) leave(c *prop.Case, _ int, in any) {
	i, _ := in.(int)
	sm := &ls.sessions[i]
	c.Classify(labelLeft)
	assert.NoError(c, sm.s.Leave(c.Context()), ls.h.Statement())
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
	ls.backed.Advance(d)
	ls.now = ls.now.Add(d)
}

// check fails c at the statement of the check when the current view differs from the
// model, or when the Done of a session is closed while the session has its lease, or open
// after the session lost its lease.
func (ls *leases) check(c *prop.Case) {
	v := current(c, ls.m)
	var want []shard.Node
	for i := range ls.sessions {
		sm := &ls.sessions[i]
		if live, _ := sm.at(ls.now); live {
			n := nodes[i]
			n.Incarnation = sm.inc
			want = append(want, n)
		}
	}
	assert.Equal(c, v.Nodes, want, ls.h.Statement(), assert.EquateEmpty())
	assert.Equal(c, v.Scope, scope, ls.h.Statement())
	assert.Equal(c, v.Incarnation, incA, ls.h.Statement())
	for i := range ls.sessions {
		sm := &ls.sessions[i]
		if sm.s == nil {
			continue
		}
		live, lost := sm.at(ls.now)
		if live && lost {
			c.Classify(labelDeadline)
		}
		if !live && !sm.left {
			c.Classify(labelExpired)
		}
		if lost {
			streamtest.Receive(c, sm.s.Done(), waitFor, ls.h.Statement())
			continue
		}
		streamtest.Blocks(c, sm.s.Done(), ls.h.Statement())
	}
}

// settle fails c at the statement of the check when the watch that sends to items did not
// yield every view up to the current view, in the order of their revisions.
func (ls *leases) settle(c *prop.Case, items <-chan streamtest.Item[membership.View]) {
	last := current(c, ls.m)
	var views []membership.View
	for range last.Revision + 1 {
		it, ok := streamtest.Receive(c, items, waitFor, ls.h.Statement())
		assert.True(c, ok, ls.h.Statement())
		assert.NoError(c, it.Err, ls.h.Statement())
		views = append(views, it.Value)
		if it.Value.Revision >= last.Revision {
			break
		}
	}
	assert.Pairwise(c, views, func(earlier, later membership.View) bool { return earlier.Revision < later.Revision },
		ls.h.Statement())
	assert.Equal(c, views[len(views)-1], last, ls.h.Statement(), assert.EquateEmpty())
}

// backend is the memory store under a planted defect of the suite, the verified fake of the
// planted defects, with the fake clock of its leases.
type backend struct {
	*memory.Store
	// clk is the clock of the leases of the store.
	clk *fake.Clock
}

// newBackend returns an empty memory store whose leases run on a fake clock at origin, and
// closes it when the check of h ends.
func newBackend(h *MembershipHandle) *backend {
	clk := fake.New(origin)
	b := &backend{Store: memory.New(clk), clk: clk}
	h.Cleanup(func() { assert.NoError(h, b.Close(), "Close of the memory store returns nil") })
	return b
}

// plantedMembership is a planted defect of the suite: the double of membership.Membership
// over the membership of a backend, with the operations of the kit on the backend.
//
// # Concurrency
//
// A plantedMembership is safe for concurrent use.
type plantedMembership struct {
	membership.Membership
	// b is the backend of the double.
	b *backend
	// points maps each loss to the point of the backend at which LoseRenewals arms a
	// fault.
	points map[Loss]memory.Point
}

// The operations of the kit that a planted membership implements.
var (
	_ Backed = plantedMembership{}
	_ Faulty = plantedMembership{}
)

// plant returns the double of the membership of b, with the options opts that replace the
// calls of the membership that break a law. LoseRenewals of the double arms the faults of
// lossPoints.
func plant(h *MembershipHandle, b *backend, opts ...MembershipStubOption) plantedMembership {
	return plantedMembership{
		Membership: NewMembershipStub(h,
			append([]MembershipStubOption{MembershipStubDelegateTo(b.Membership())}, opts...)...),
		b:      b,
		points: lossPoints,
	}
}

// Store returns the memory store of the backend.
func (p plantedMembership) Store() store.Store {
	return p.b.Store
}

// Advance moves the clock of the backend forward by d.
func (p plantedMembership) Advance(d time.Duration) {
	p.b.clk.Advance(d)
}

// LoseRenewals arms the fault of the backend at the point of l.
func (p plantedMembership) LoseRenewals(l Loss) {
	p.b.FailAt(p.points[l])
}

// ViewsListTheLiveSessions checks that each view of Watch lists the node of each session
// whose lease is live, and Done closes once a session loses its lease.
//
// A machine joins, leaves and stops the renewals of nodes, and advances the clock of a
// membership that implements Backed. After every step it checks the current view and the
// Done of each session. At the end it checks that a watch from the start yielded every
// view in the order of their revisions. An application requires a share of the cases for
// each event of a lease.
//
//dokimi:check Membership.Join defect=keepsLeftNodes options=leaseOptions
func ViewsListTheLiveSessions(h *MembershipHandle, c *prop.Case) {
	backed, ok := operation[Backed](h)
	if !ok {
		return
	}
	m := h.Subject()
	ls := &leases{h: h, m: m, backed: backed, now: origin}
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, m.Watch(ctx, scope, incA))
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions:   ls.actions(),
		Invariant: func(c *prop.Case, _ struct{}) { ls.check(c) },
		Settle:    func(c *prop.Case, _ struct{}) { ls.settle(c, items) },
	}, stateful.Swarm(false))
}

// leaseOptions returns the least share of the cases of the machine of leases that count
// each event of a lease.
func leaseOptions(*MembershipHandle) []prop.Option {
	return []prop.Option{
		prop.Require(labelLeft, shareFrequent),
		prop.Require(labelRejoined, shareFrequent),
		prop.Require(labelExpired, shareCommon),
		prop.Require(labelDeadline, shareCommon),
	}
}

// keepsLeftNodes returns a planted defect whose sessions return nil from Leave without
// ending, so the views keep the node of a session that left.
func keepsLeftNodes(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		n *shard.Node, l ownership.Lease,
	) (membership.Session, error) {
		ss, err := b.Membership().Join(ctx, s, inc, n, l)
		if err != nil {
			return ss, err
		}
		return NewSessionStub(h, SessionStubDelegateTo(ss), WithSessionLeave(func(context.Context) error {
			return nil
		})), nil
	}))
}

// JoinsRefuseOtherIncarnations checks that Join returns ErrIncarnation for a scope that
// another incarnation created.
//
//dokimi:check Membership.Join defect=joinsOtherIncarnations
func JoinsRefuseOtherIncarnations(h *MembershipHandle) {
	backed, ok := operation[Backed](h)
	if !ok {
		return
	}
	create(h, backed.Store())
	n := nodes[0]
	_, err := h.Subject().Join(h.Context(), scope, incB, &n, lease)
	assert.ErrorIs(h, err, shard.ErrIncarnation, h.Statement())
}

// joinsOtherIncarnations returns a planted defect whose Join of a scope that another
// incarnation created joins the node in the incarnation of the scope.
func joinsOtherIncarnations(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		n *shard.Node, l ownership.Lease,
	) (membership.Session, error) {
		ss, err := b.Membership().Join(ctx, s, inc, n, l)
		if !errors.Is(err, shard.ErrIncarnation) {
			return ss, err
		}
		head, err := b.Head(ctx, s)
		if err != nil {
			return nil, err
		}
		return b.Membership().Join(ctx, s, head.Incarnation, n, l)
	}))
}

// JoinsRefuseNodesWithoutAnID checks that Join returns an error of the class Invalid for a
// node without an ID.
//
//dokimi:check Membership.Join defect=namesAnonymousNodes
func JoinsRefuseNodesWithoutAnID(h *MembershipHandle) {
	n := nodes[0]
	n.ID = ""
	_, err := h.Subject().Join(h.Context(), scope, incA, &n, lease)
	assert.Equal(h, errs.Classify(err), errs.Invalid, h.Statement())
}

// namesAnonymousNodes returns a planted defect whose Join gives a node without an ID the ID
// anonymous.
func namesAnonymousNodes(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		n *shard.Node, l ownership.Lease,
	) (membership.Session, error) {
		if n.ID == "" {
			named := *n
			named.ID = anonymous
			n = &named
		}
		return b.Membership().Join(ctx, s, inc, n, l)
	}))
}

// JoinsRefuseInvalidLeases checks that Join returns an error of the class Invalid for a
// lease that the configuration refuses.
//
//dokimi:check Membership.Join defect=replacesInvalidLeases
func JoinsRefuseInvalidLeases(h *MembershipHandle) {
	n := nodes[0]
	invalid := lease
	invalid.Renew = invalid.TTL
	_, err := h.Subject().Join(h.Context(), scope, incA, &n, invalid)
	assert.Equal(h, errs.Classify(err), errs.Invalid, h.Statement())
}

// replacesInvalidLeases returns a planted defect whose Join replaces a lease that the
// configuration refuses with the lease of the machine of leases.
func replacesInvalidLeases(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		n *shard.Node, l ownership.Lease,
	) (membership.Session, error) {
		if err := (ownership.Config{Lease: l, Fenced: true}).Validate(); err != nil {
			l = lease
		}
		return b.Membership().Join(ctx, s, inc, n, l)
	}))
}

// JoinsLeaveTheNodeUnchanged checks that Join does not modify the node that it registers.
//
//dokimi:check Membership.Join defect=stampsNodes
func JoinsLeaveTheNodeUnchanged(h *MembershipHandle) {
	m := h.Subject()
	n := nodes[0].CloneKanon()
	assert.Pure(h, func() *shard.Node { return n.CloneKanon() }, func() {
		_, err := m.Join(h.Context(), scope, incA, n, lease)
		assert.NoError(h, err, "Join registers a node")
	}, h.Statement())
}

// stampsNodes returns a planted defect whose Join writes the incarnation of the session
// into the node of the caller.
func stampsNodes(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		n *shard.Node, l ownership.Lease,
	) (membership.Session, error) {
		ss, err := b.Membership().Join(ctx, s, inc, n, l)
		if err == nil {
			n.Incarnation = ss.Incarnation()
		}
		return ss, err
	}))
}

// ViewsCopyTheJoinedNode checks that a view does not change when the caller changes the
// node of a join after the join.
//
//dokimi:check Membership.Join defect=sharesNodes
func ViewsCopyTheJoinedNode(h *MembershipHandle) {
	m := h.Subject()
	n := nodes[0].CloneKanon()
	ss, err := m.Join(h.Context(), scope, incA, n, lease)
	assert.NoError(h, err, "Join registers a node")
	want := nodes[0]
	want.Incarnation = ss.Incarnation()
	n.Address = nodes[1].Address
	n.Capacity[cpu] = nodes[1].Capacity[cpu]
	n.Labels[0] = nodes[1].Labels[0]
	assert.Equal(h, current(h, m).Nodes, []shard.Node{want}, h.Statement())
}

// sharesNodes returns a planted defect whose views list the node of the caller of each
// join, with the incarnation of its session.
func sharesNodes(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	var mu sync.Mutex
	joined := make(map[shard.NodeID]*shard.Node)
	return plant(h, b,
		WithMembershipJoin(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
			n *shard.Node, l ownership.Lease,
		) (membership.Session, error) {
			mu.Lock()
			joined[n.ID] = n
			mu.Unlock()
			return b.Membership().Join(ctx, s, inc, n, l)
		}),
		WithMembershipWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		) iter.Seq2[membership.View, error] {
			views := b.Membership().Watch(ctx, s, inc)
			return func(yield func(membership.View, error) bool) {
				for v, err := range views {
					v.Nodes = slices.Clone(v.Nodes)
					mu.Lock()
					for i := range v.Nodes {
						if n, ok := joined[v.Nodes[i].ID]; ok {
							incarnation := v.Nodes[i].Incarnation
							v.Nodes[i] = *n
							v.Nodes[i].Incarnation = incarnation
						}
					}
					mu.Unlock()
					if !yield(v, err) {
						return
					}
				}
			}
		}))
}

// WatchesRefuseOtherIncarnations checks that Watch yields ErrIncarnation for a scope that
// another incarnation created, and ends.
//
//dokimi:check Membership.Watch defect=watchesOtherIncarnations
func WatchesRefuseOtherIncarnations(h *MembershipHandle) {
	backed, ok := operation[Backed](h)
	if !ok {
		return
	}
	create(h, backed.Store())
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, h.Subject().Watch(ctx, scope, incB))
	it, ok := streamtest.Receive(h, items, waitFor, h.Statement())
	assert.True(h, ok, h.Statement())
	assert.ErrorIs(h, it.Err, shard.ErrIncarnation, h.Statement())
	streamtest.End(h, items, waitFor, h.Statement())
}

// watchesOtherIncarnations returns a planted defect whose watch of a scope that another
// incarnation created when the watch starts yields the views of the incarnation of the
// scope.
func watchesOtherIncarnations(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
	) iter.Seq2[membership.View, error] {
		head, err := b.Head(ctx, s)
		if err == nil && !head.Incarnation.IsZero() && head.Incarnation != inc {
			return b.Membership().Watch(ctx, s, head.Incarnation)
		}
		return b.Membership().Watch(ctx, s, inc)
	}))
}

// WatchesRefuseCreationsByOtherIncarnations checks that Watch yields ErrIncarnation when
// another incarnation creates the scope, and ends.
//
//dokimi:check Membership.Watch defect=endsAtCreations
func WatchesRefuseCreationsByOtherIncarnations(h *MembershipHandle) {
	backed, ok := operation[Backed](h)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, h.Subject().Watch(ctx, scope, incB))
	it, ok := streamtest.Receive(h, items, waitFor, "Watch yields the current view within the wait of the suite")
	assert.True(h, ok, "Watch yields the current view of a scope before its first commit")
	assert.NoError(h, it.Err, "Watch yields the current view of a scope before its first commit")
	create(h, backed.Store())
	it, ok = streamtest.Receive(h, items, waitFor, h.Statement())
	assert.True(h, ok, h.Statement())
	assert.ErrorIs(h, it.Err, shard.ErrIncarnation, h.Statement())
	streamtest.End(h, items, waitFor, h.Statement())
}

// endsAtCreations returns a planted defect whose watch ends without an error when another
// incarnation creates the scope after the watch yielded a view.
func endsAtCreations(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
	) iter.Seq2[membership.View, error] {
		views := b.Membership().Watch(ctx, s, inc)
		return func(yield func(membership.View, error) bool) {
			yielded := false
			for v, err := range views {
				if yielded && errors.Is(err, shard.ErrIncarnation) {
					return
				}
				yielded = err == nil
				if !yield(v, err) {
					return
				}
			}
		}
	}))
}

// WatchesEndWithTheirContext checks that Watch ends once its context ends.
//
//dokimi:check Membership.Watch defect=ignoresWatchContexts
func WatchesEndWithTheirContext(h *MembershipHandle) {
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, h.Subject().Watch(ctx, scope, incA))
	_, ok := streamtest.Receive(h, items, waitFor, "Watch yields the current view within the wait of the suite")
	assert.True(h, ok, "Watch yields the current view")
	cancel()
	streamtest.End(h, items, waitFor, h.Statement())
}

// ignoresWatchContexts returns a planted defect whose watch continues after its context
// ends.
func ignoresWatchContexts(h *MembershipHandle) membership.Membership {
	b := newBackend(h)
	return plant(h, b, WithMembershipWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
	) iter.Seq2[membership.View, error] {
		return b.Membership().Watch(context.WithoutCancel(ctx), s, inc)
	}))
}

// LostRenewalsEndTheSession checks that a session whose renewals a fault loses closes Done
// at the deadline of its join, and its node leaves the view at the expiry of the join.
//
//dokimi:check Membership.Join defect=appliesLostRenewals
func LostRenewalsEndTheSession(h *MembershipHandle) {
	losesRenewals(h, LoseRequest, 0)
}

// appliesLostRenewals returns a planted defect whose lost renewals apply, as renewals whose
// replies are lost do.
func appliesLostRenewals(h *MembershipHandle) membership.Membership {
	p := plant(h, newBackend(h))
	p.points = map[Loss]memory.Point{LoseRequest: memory.PointRenewAck, LoseReply: memory.PointRenewAck}
	return p
}

// LostRepliesEndTheSession checks that a session whose renewal replies a fault loses closes
// Done at the deadline of its join, and the view lists its node until the expiry of its last
// renewal.
//
//dokimi:check Membership.Join defect=dropsRenewalsOfLostReplies
func LostRepliesEndTheSession(h *MembershipHandle) {
	losesRenewals(h, LoseReply, 1)
}

// dropsRenewalsOfLostReplies returns a planted defect whose renewals with lost replies do
// not apply, as lost renewals do.
func dropsRenewalsOfLostReplies(h *MembershipHandle) membership.Membership {
	p := plant(h, newBackend(h))
	p.points = map[Loss]memory.Point{LoseRequest: memory.PointRenewApply, LoseReply: memory.PointRenewApply}
	return p
}

// operation returns the subject of h as the operation O of the kit. It reports false, and
// ends the check with a fault that wraps ErrOperation, when the subject does not implement
// O.
func operation[O any](h *MembershipHandle) (O, bool) {
	o, ok := h.Subject().(O)
	if !ok {
		assert.Fault(h, fmt.Errorf("%w: %T does not implement %v", ErrOperation, h.Subject(), reflect.TypeFor[O]()))
	}
	return o, ok
}

// losesRenewals checks that a session whose renewals the fault l interrupts closes its Done
// at the deadline of its join, and that the view lists its node until the expiry of the last
// renewal that applied. listed is the number of nodes that the view lists at the expiry of
// the join: one when the renewals applied, and none otherwise.
func losesRenewals(h *MembershipHandle, l Loss, listed int) {
	backed, ok := operation[Backed](h)
	if !ok {
		return
	}
	faulty, ok := operation[Faulty](h)
	if !ok {
		return
	}
	m := h.Subject()
	n := nodes[0]
	ss, err := m.Join(h.Context(), scope, incA, &n, shortLease)
	assert.NoError(h, err, "Join registers a node")
	for range lostRenewals {
		faulty.LoseRenewals(l)
	}
	backed.Advance(beforeDeadline)
	assert.Length(h, current(h, m).Nodes, 1, h.Statement())
	streamtest.Blocks(h, ss.Done(), h.Statement())
	backed.Advance(afterDeadline - beforeDeadline)
	assert.Length(h, current(h, m).Nodes, 1, h.Statement())
	streamtest.Receive(h, ss.Done(), waitFor, h.Statement())
	backed.Advance(atExpiry - afterDeadline)
	assert.Length(h, current(h, m).Nodes, listed, h.Statement())
	backed.Advance(atLateExpiry - atExpiry)
	assert.Empty(h, current(h, m).Nodes, h.Statement())
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
