// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// campaignsCell is the cell of the scope in which each campaign joins its lease. No cell of a
// namespace has an ID of this form, and no machine reads the scope.
const campaignsCell shard.CellID = "campaigns"

// waitKey is the identity of the wait of an effect: the name of its process, the epoch of the
// process and the token of the effect.
type waitKey struct {
	// process is the name of the process of the effect.
	process string
	// epoch is the epoch of the process when it returned the effect.
	epoch uint64
	// token is the token of the effect.
	token machine.Token
}

// campaign is the candidacy of a Campaign: the lease that bounds it, the cancel of the context
// of the lease and the candidacy, and its term once the election grants it.
type campaign struct {
	// cancel cancels the context of the lease and the candidacy, which ends the term at the
	// next call into the store.
	cancel context.CancelFunc
	// lease is the session that the campaign joined as its lease. The candidacy ends once the
	// lease ends.
	lease *joining
	// cand is the candidacy in the election of the store.
	cand *memory.Candidacy
	// lead is the term that the election granted, and nil before the grant.
	lead store.Leadership
	// key is the key of the wait of the campaign.
	key waitKey
	// done reports that the wait of the campaign has ended.
	done bool
}

// joining is a session that a Join started, with its lease and the monotonic times of the clock
// of its host at its last renewal, at its last accepted renewal and at its local deadline. A
// node measures the validity of its lease on the monotonic time of its clock, which the jumps
// and the strobe of the clock do not move.
type joining struct {
	// session is the session.
	session membership.Session
	// lease is the lease of the session.
	lease ownership.Lease
	// renewed is the monotonic time of the clock of the host at the start or the last renewal of
	// the session.
	renewed time.Duration
	// accepted is the monotonic time of the clock of the host at the start of the session or at
	// its last renewal that the membership accepted.
	accepted time.Duration
	// valid is the local deadline of the session on the monotonic time of the clock of the host.
	valid time.Duration
}

// renew renews the session of j at each Renew of its lease on the clock of the host of p while
// ctx has not ended, and moves the local deadline of j to the validity of the lease after each
// renewal that the membership accepts. A timer of World.AfterFunc at each renewal, and at the
// deadline after a renewal that fails, makes the world poll the wait of the session at those
// times without an event to a machine. Once the deadline has passed, renew ends the session at
// its node. Under the variant failedRenewals, a renewal that fails moves the deadline too.
func (s *env) renew(ctx context.Context, w *world, p *process, j *joining) {
	every := time.Duration(j.lease.Renew)
	if now := p.Clock().Monotonic(); ctx.Err() == nil && now-j.renewed >= every {
		j.renewed = now
		// A renewal that fails does not move the deadline of the session, as a failed renewal
		// of the loop of the store does not.
		err := s.members.Renew(j.session)
		if err == nil {
			j.accepted = now
		}
		if err == nil || s.variant.failedRenewals {
			j.valid = now + j.lease.Validity()
		} else if left := j.valid - now; left > 0 && left < every {
			w.AfterFunc(p, left, func() {})
		}
		w.AfterFunc(p, every, func() {})
	}
	if p.Clock().Monotonic() >= j.valid {
		assert.NoError(s.tb, s.members.Lose(j.session), "the membership of a world must end its own session")
	}
}

// waits is the state of the waits of the effects of a world.
type waits struct {
	// joins maps each join whose session has not ended to its session.
	joins map[waitKey]*joining
	// campaigns lists the candidacies whose waits and leases have not ended, in the order of
	// their starts.
	campaigns []*campaign
}

// pump applies the due events of the leases of the store. It then ends each candidacy whose
// lease has ended, and applies the ends of their terms.
func (s *env) pump() {
	s.store.Pump()
	ended := false
	s.waits.campaigns = slices.DeleteFunc(s.waits.campaigns, func(c *campaign) bool {
		if c.done {
			return true
		}
		select {
		case <-c.lease.session.Done():
			c.cancel()
			ended = true
			return true
		default:
			return false
		}
	})
	if ended {
		s.store.Pump()
	}
}

// delivered returns the error of the result of a message: an error that wraps
// protocol.ErrUnreachable for a call whose process crashed or stopped before it replied, as a
// transport reports a process that does not reply, and err otherwise.
func delivered(err error) error {
	if errors.Is(err, sim.ErrCrashed) || errors.Is(err, sim.ErrRunEnded) {
		return fmt.Errorf("%w: %w", protocol.ErrUnreachable, err)
	}
	return err
}

// controllerOf returns the name of the process at the address of the controller of the record
// c that serves the calls of the scope: the cell machine of the cell of the scope, and the root
// of its namespace for the root scope. It returns an error that wraps protocol.ErrUnreachable
// for an address of no host of s, and for a process that the world does not have, as a
// transport reports an address without a server.
func (s *env) controllerOf(w *world, c *shard.Cell, scope shard.Scope) (string, error) {
	name, ok := strings.CutSuffix(c.Controller, port)
	a := address{host: name, namespace: scope.Namespace, role: roleRoot}
	if scope.Cell != "" {
		a.role, a.cell = roleCell, scope.Cell
	}
	to := a.String()
	if !ok || !slices.ContainsFunc(s.hosts, func(h *host) bool { return h.name == name }) || w.Process(to) == nil {
		return "", fmt.Errorf("%w: no controller of %v at %q", protocol.ErrUnreachable, scope, c.Controller)
	}
	return to, nil
}

// performHead reads the header of the scope of f. Under the variant schemaless, the header has
// at most the schema that this release writes.
func performHead(w *world, p *process, f machine.Head) outcome {
	s := sim.Dependency[*env](w, p)
	s.pump()
	h, err := s.store.Head(w.Context(), f.Scope)
	if s.variant.schemaless {
		h.Schema = min(h.Schema, store.Schema)
	}
	return outcome{Event: machine.HeadRead{Head: h, Err: err, Token: f.Token}}
}

// performRead reads the page of f. It reads the placements of the page and the token of the
// next page from the cursor of the read, and then closes the cursor, as the driver does. A
// PageRead with an error has only the error and the token.
func performRead(w *world, p *process, f *machine.Read) outcome {
	s := sim.Dependency[*env](w, p)
	s.pump()
	ctx := w.Context()
	st, cur, err := s.store.Read(ctx, f.Scope, f.Incarnation, f.Query, f.Page)
	if err != nil {
		return outcome{Event: machine.PageRead{Err: err, Token: f.Token}}
	}
	var placements []shard.Placement
	for pl, perr := range cur.Seq(ctx) {
		if perr != nil {
			err = perr
			break
		}
		placements = append(placements, pl)
	}
	next := cur.NextPage()
	err = errors.Join(err, cur.Close())
	if err != nil {
		return outcome{Event: machine.PageRead{Err: err, Token: f.Token}}
	}
	return outcome{Event: machine.PageRead{State: st, Placements: placements, Next: next, Token: f.Token}}
}

// performWatch waits on the generations of the scope of f after f.From. Each poll reads the
// next generation from a watch of the store under a context that has ended, so the read does
// not wait, and yields it as a Watched. The wait ends at its first error, and without an event
// at its cancel, which the world calls at the Unwatch of f.
func performWatch(w *world, p *process, f *machine.Watch) outcome {
	s := sim.Dependency[*env](w, p)
	ended, cancel := context.WithCancel(w.Context())
	cancel()
	from, cancelled := f.From, false
	return outcome{
		Next: func() (machine.Event, bool, bool) {
			if cancelled {
				return nil, false, true
			}
			s.pump()
			for ev, err := range s.store.Watch(ended, f.Scope, f.Incarnation, from, f.Node) {
				if err != nil {
					return machine.Watched{Err: err, Token: f.Token}, true, true
				}
				from = ev.Generation
				return machine.Watched{Event: ev, Token: f.Token}, true, false
			}
			return nil, false, false
		},
		Cancel: func() { cancelled = true },
	}
}

// performCommit commits the mutation of f through the writer of the store for the scope, the
// incarnation and the term of f, or through the writer of the variant of the env. The env
// records each commit that creates a new generation of its scope, with the header of the scope
// before it, also a commit whose reply the store loses after it applies. The store returns the
// receipt of an applied commit to a retry of the commit, and that retry changes nothing.
func performCommit(w *world, p *process, f *machine.Commit) outcome {
	s := sim.Dependency[*env](w, p)
	s.pump()
	ctx := w.Context()
	before, err := s.store.Head(ctx, f.Scope)
	assert.NoError(s.tb, err, "the store of a world must read the header of a scope")
	writer := s.store.Writer(f.Scope, f.Incarnation, f.Term)
	if s.variant.writer != nil {
		writer = s.variant.writer(s, f, before)
	}
	g, err := writer.Commit(ctx, f.Expect, f.Mutation)
	after, herr := s.store.Head(ctx, f.Scope)
	assert.NoError(s.tb, herr, "the store of a world must read the header of a scope")
	if after.Generation > before.Generation {
		s.commits = append(s.commits, commit{
			mutation:    f.Mutation.CloneKanon(),
			scope:       f.Scope,
			before:      before,
			incarnation: f.Incarnation,
			term:        f.Term,
			expect:      f.Expect,
			generation:  after.Generation,
		})
	}
	return outcome{Event: machine.Committed{Generation: g, Err: err, Token: f.Token}}
}

// performOutcome reads the receipt of the commit of f.
func performOutcome(w *world, p *process, f *machine.Outcome) outcome {
	s := sim.Dependency[*env](w, p)
	s.pump()
	r, found, err := s.store.Outcome(w.Context(), f.Scope, f.ID)
	return outcome{Event: machine.OutcomeRead{Receipt: r, Found: found, Err: err, Token: f.Token}}
}

// performCampaign joins the node of the host in the scope of the campaigns of the namespace of
// f, with the lease of the ownership of the namespace. It then enters a candidacy of the
// process in the election of the scope of f, and waits on it. The candidacy ends once the
// session of the join ends, as the lease of a client of a store bounds the keys of its
// elections. The wait renews the session at each Renew of the lease on the clock of the host,
// and ends it at its local deadline on that clock, so the session of a crashed or paused host
// ends at its expiry. The cancel of the wait ends the renewals and the candidacy, as the driver
// cancels the context of a campaign. The wait yields these events:
//   - The Campaigned of the grant, and the TermEnded of the term once the term ends. A grant
//     before the cancel yields both, as the driver passes a term that the store granted while
//     the campaign stopped.
//   - A Campaigned with the error of its context for a cancel before the grant, as the driver
//     passes it.
//   - A Campaigned with an error that wraps version.ErrOutcomeUnknown when the session ends
//     before the grant, as the store fails a campaign that it ended.
//
// performCampaign returns a Campaigned with the error of the store for a join or a candidacy
// that the store refuses.
func performCampaign(w *world, p *process, f machine.Campaign) outcome {
	s, h := sim.Dependency[*env](w, p), sim.Dependency[*host](w, p)
	s.pump()
	lease := s.leases[f.Scope.Namespace]
	ctx, cancel := context.WithCancel(w.Context())
	scope := shard.Scope{Namespace: f.Scope.Namespace, Cell: campaignsCell}
	ss, err := s.members.Join(ctx, scope, shard.NamespaceIncarnation{}, &h.node, lease)
	var cand *memory.Candidacy
	if err == nil {
		cand, err = s.store.Candidate(ctx, f.Scope)
	}
	if err != nil {
		cancel()
		return outcome{Event: machine.Campaigned{Err: err, Token: f.Token}}
	}
	now := p.Clock().Monotonic()
	c := &campaign{
		cancel: cancel,
		lease:  &joining{session: ss, lease: lease, renewed: now, accepted: now, valid: now + lease.Validity()},
		cand:   cand,
		key:    waitKey{process: p.Name(), epoch: p.Epoch(), token: f.Token},
	}
	s.waits.campaigns = append(s.waits.campaigns, c)
	w.AfterFunc(p, time.Duration(lease.Renew), func() {})
	cancelled := false
	return outcome{
		Next: func() (machine.Event, bool, bool) {
			s.pump()
			s.renew(ctx, w, p, c.lease)
			s.pump()
			if c.lead != nil {
				select {
				case <-c.lead.Done():
					c.done = true
					cancel()
					return machine.TermEnded{Token: f.Token}, true, true
				default:
					return nil, false, false
				}
			}
			select {
			case <-cand.Granted():
				l, granted := cand.Lead()
				assert.True(s.tb, granted, "a candidacy whose grant has closed must have its term")
				c.lead = l
				return machine.Campaigned{Term: l.Term(), Token: f.Token}, true, false
			default:
			}
			if cancelled {
				c.done = true
				return machine.Campaigned{Err: context.Canceled, Token: f.Token}, true, true
			}
			select {
			case <-ss.Done():
				c.done = true
				cancel()
				err := fmt.Errorf("%w: the session of %s in %v ended before the grant", version.ErrOutcomeUnknown,
					h.name, scope)
				return machine.Campaigned{Err: err, Token: f.Token}, true, true
			default:
				return nil, false, false
			}
		},
		Cancel: func() {
			cancelled = true
			cancel()
		},
	}
}

// performResign resigns the term that the store granted to the campaign of the token of f.
// The world then calls the cancel of the wait of the campaign, which ends the candidacy, so a
// failed resignation still ends the term. A campaign without a term resigns nothing.
func performResign(w *world, p *process, f machine.Resign) outcome {
	s := sim.Dependency[*env](w, p)
	key := waitKey{process: p.Name(), epoch: p.Epoch(), token: f.Token}
	at := slices.IndexFunc(s.waits.campaigns, func(c *campaign) bool { return c.key == key })
	if at < 0 {
		return outcome{}
	}
	if l, granted := s.waits.campaigns[at].cand.Lead(); granted {
		// The cancel of the candidacy ends the term whatever Resign returns.
		_ = l.Resign(w.Context())
	}
	return outcome{}
}

// performJoin registers the node of f in the scope of f, and waits on the session. The wait
// yields the Joined of the session, renews the session at each Renew of its lease on the clock
// of its host, and yields the SessionEnded of the session once the session ends. It ends the
// session at its local deadline on the monotonic time of its host, as the membership client of
// a node does. A paused host advances neither its clock nor its timers, so it does not renew.
// The cancel of the wait ends the renewals, as the driver cancels the context of a join, so the
// session ends at its deadline. The env records the namespace incarnation of each session that
// the membership grants, with the incarnation of its scope at the grant.
func performJoin(w *world, p *process, f *machine.Join) outcome {
	s := sim.Dependency[*env](w, p)
	s.pump()
	ctx, cancel := context.WithCancel(w.Context())
	ss, err := s.members.Join(ctx, f.Scope, f.Incarnation, f.Node, f.Lease)
	if err != nil {
		cancel()
		return outcome{Event: machine.Joined{Err: err, Token: f.Token}}
	}
	key := waitKey{process: p.Name(), epoch: p.Epoch(), token: f.Token}
	now := p.Clock().Monotonic()
	j := &joining{session: ss, lease: f.Lease, renewed: now, accepted: now, valid: now + f.Lease.Validity()}
	s.waits.joins[key] = j
	head, err := s.store.Head(ctx, f.Scope)
	assert.NoError(s.tb, err, "the store of a world must read the header of a scope")
	s.joined = append(s.joined, joinedSession{scope: f.Scope, incarnation: f.Incarnation, current: head.Incarnation})
	if f.Scope.Cell != "" {
		s.labels[nodeKey{scope: f.Scope, node: f.Node.ID, incarnation: ss.Incarnation()}] = slices.Clone(f.Node.Labels)
	}
	w.AfterFunc(p, time.Duration(f.Lease.Renew), func() {})
	joined := false
	return outcome{
		Next: func() (machine.Event, bool, bool) {
			if !joined {
				joined = true
				return machine.Joined{Incarnation: ss.Incarnation(), Token: f.Token}, true, false
			}
			s.pump()
			s.renew(ctx, w, p, j)
			select {
			case <-ss.Done():
				cancel()
				delete(s.waits.joins, key)
				return machine.SessionEnded{Token: f.Token}, true, true
			default:
				return nil, false, false
			}
		},
		Cancel: cancel,
	}
}

// performLeave leaves the session of the join of the token of f through the membership. The
// world then calls the cancel of the wait of the join, which ends the renewals of the session,
// so a session whose Leave failed still ends at its deadline. The wait of the join then yields
// the SessionEnded of the session. A Leave of a join whose session has ended has no result, as
// in the driver.
func performLeave(w *world, p *process, f machine.Leave) outcome {
	s := sim.Dependency[*env](w, p)
	j, ok := s.waits.joins[waitKey{process: p.Name(), epoch: p.Epoch(), token: f.Token}]
	if !ok {
		return outcome{}
	}
	return outcome{Event: machine.Left{Err: j.session.Leave(w.Context()), Token: f.Token}}
}

// performWatchViews waits on the views of the scope of f. Each poll reads the views that the
// reader has not read, and yields them as Viewed events, one at each poll. The wait ends at its
// first error, and without an event at its cancel, which the world calls at the Unwatch of f.
// Either end closes its reader.
func performWatchViews(w *world, p *process, f *machine.WatchViews) outcome {
	s := sim.Dependency[*env](w, p)
	r := s.members.Views(f.Scope, f.Incarnation)
	var pending []membership.View
	cancelled := false
	return outcome{
		Next: func() (machine.Event, bool, bool) {
			if cancelled {
				return nil, false, true
			}
			if len(pending) == 0 {
				s.pump()
				views, _, err := r.Next()
				if err != nil {
					r.Close()
					return machine.Viewed{Err: err, Token: f.Token}, true, true
				}
				pending = views
			}
			if len(pending) == 0 {
				return nil, false, false
			}
			v := pending[0]
			pending = slices.Delete(pending, 0, 1)
			return machine.Viewed{View: v, Token: f.Token}, true, false
		},
		Cancel: func() {
			cancelled = true
			r.Close()
		},
	}
}

// performCallback sends the request of f to the agent of the node of f for the scope of the
// request: the agent of the root for the root scope, and the agent of the node for a cell. The
// call has the metadata of the host, and its result has the class that the agent gives the
// error of an attempt. The env records a copy of the request with the number of the commits
// of the store and the identity of the host, for the invariants and for the action forge. It
// marks the record once the controller has a reply without an error.
func performCallback(w *world, p *process, f machine.Callback) outcome {
	h := sim.Dependency[*host](w, p)
	at := len(h.sys.callbacks)
	h.sys.callbacks = append(h.sys.callbacks, callback{
		request: f.Request.CloneKanon(),
		caller:  h.node.Identity,
		commits: len(h.sys.commits),
	})
	md := protocol.Metadata{Caller: h.node.Identity, Version: protocol.Current}
	if err := md.Check(); err != nil {
		return outcome{Event: machine.Called{Err: err, Token: f.Token}}
	}
	a := address{host: string(f.Node.ID), namespace: f.Request.Scope.Namespace, role: roleAgent}
	if f.Request.Scope.Cell == "" {
		a.role = roleRootAgent
	}
	to := a.String()
	if w.Process(to) == nil {
		err := fmt.Errorf("%w: no agent of %s in %v", protocol.ErrUnreachable, f.Node.ID, f.Request.Scope)
		return outcome{Event: machine.Called{Err: err, Token: f.Token}}
	}
	req := f.Request.CloneKanon()
	return outcome{Message: &sim.Message[machine.Event]{
		Call: func(reply uint64) machine.Event {
			return machine.Do{Request: req, Metadata: md, Reply: machine.ReplyToken(reply)}
		},
		Result: func(_ any, err error) machine.Event {
			if err == nil {
				h.sys.callbacks[at].replied = true
			}
			return machine.Called{Err: machine.ClassifyAttempt(delivered(err)), Token: f.Token}
		},
		To: to,
	}}
}

// performSendReport sends the report of f to the controller at the address of the record of
// the cell of f.
func performSendReport(w *world, p *process, f machine.SendReport) outcome {
	s, h := sim.Dependency[*env](w, p), sim.Dependency[*host](w, p)
	md := protocol.Metadata{Caller: h.node.Identity, Version: protocol.Current}
	err := md.Check()
	var to string
	if err == nil {
		to, err = s.controllerOf(w, f.Cell, f.Report.Scope)
	}
	if err != nil {
		return outcome{Event: machine.Reported{Err: err, Token: f.Token}}
	}
	report := f.Report.CloneKanon()
	return outcome{Message: &sim.Message[machine.Event]{
		Call: func(reply uint64) machine.Event {
			return machine.Report{Report: report, Metadata: md, Reply: machine.ReplyToken(reply)}
		},
		Result: func(v any, err error) machine.Event {
			ack, _ := v.(protocol.Ack)
			return machine.Reported{Ack: ack, Err: delivered(err), Token: f.Token}
		},
		To: to,
	}}
}

// performSendDisrupt sends the disruption of f to the controller at the address of the record
// of the cell of f.
func performSendDisrupt(w *world, p *process, f machine.SendDisrupt) outcome {
	s, h := sim.Dependency[*env](w, p), sim.Dependency[*host](w, p)
	md := protocol.Metadata{Caller: h.node.Identity, Version: protocol.Current}
	err := md.Check()
	var to string
	if err == nil {
		to, err = s.controllerOf(w, f.Cell, f.Disruption.Scope)
	}
	if err != nil {
		return outcome{Event: machine.Decided{Err: err, Token: f.Token}}
	}
	d := f.Disruption.CloneKanon()
	deadline := p.Clock().Now().Add(f.Timeout)
	return outcome{Message: &sim.Message[machine.Event]{
		Call: func(reply uint64) machine.Event {
			return machine.Disrupt{Disruption: d, Metadata: md, Deadline: deadline, Reply: machine.ReplyToken(reply)}
		},
		Result: func(v any, err error) machine.Event {
			decision, _ := v.(budget.Decision)
			return machine.Decided{Decision: decision, Err: delivered(err), Token: f.Token}
		},
		To: to,
	}}
}

// performSendDone sends the end of the reservation of f to the controller at the address of
// the record of the cell of f.
func performSendDone(w *world, p *process, f machine.SendDone) outcome {
	s, h := sim.Dependency[*env](w, p), sim.Dependency[*host](w, p)
	md := protocol.Metadata{Caller: h.node.Identity, Version: protocol.Current}
	err := md.Check()
	var to string
	if err == nil {
		to, err = s.controllerOf(w, f.Cell, f.Args.Scope)
	}
	if err != nil {
		return outcome{Event: machine.Ended{Err: err, Token: f.Token}}
	}
	args := *f.Args
	return outcome{Message: &sim.Message[machine.Event]{
		Call: func(reply uint64) machine.Event {
			return machine.Done{Args: &args, Metadata: md, Reply: machine.ReplyToken(reply)}
		},
		Result: func(_ any, err error) machine.Event { return machine.Ended{Err: delivered(err), Token: f.Token} },
		To:     to,
	}}
}

// performHandle calls the handler of f. The agent of the root starts and stops the cell
// machines of its host. The agent of a node calls the handler of the reference application:
// AddShard for an add and DropShard for a drop. A request with any other call has an error
// that wraps protocol.ErrUnsupported, as in the driver.
func performHandle(w *world, p *process, f *machine.Handle) outcome {
	if sim.Dependency[role](w, p) == roleRootAgent {
		return sim.Dependency[*host](w, p).handleCell(p, f)
	}
	s := sim.Dependency[*env](w, p)
	var err error
	switch f.Request.Call {
	case protocol.Add:
		err = s.app.AddShard(f.Request, &f.Fence)
	case protocol.Drop:
		s.app.DropShard(f.Request, &f.Fence)
	default:
		err = fmt.Errorf("%w: the call %v", protocol.ErrUnsupported, f.Request.Call)
	}
	return outcome{Event: machine.Handled{Err: err, Token: f.Token}}
}

// hostedCell is a cell machine on a host, as the driver of a controller process runs it. The
// driver passes Started itself and waits for its reply, so the end of the wait of an add does
// not end the wait of the driver. hostedCell records the reply of the machine to Started in the
// record of its start, and does not pass the machine the cancel of that Started.
type hostedCell struct {
	machine.Machine
	// hosted is the record of the start of the machine.
	hosted *hosted
}

// Step passes ev to the cell machine, apart from the cancel of its Started, and records the reply
// of the machine to its Started.
func (c hostedCell) Step(ev machine.Event, out []machine.Effect) []machine.Effect {
	if cancel, cancels := ev.(machine.Cancelled); cancels && cancel.Reply == c.hosted.started {
		return out
	}
	n := len(out)
	out = c.Machine.Step(ev, out)
	for _, f := range out[n:] {
		if r, replies := f.(machine.Reply[struct{}]); replies && r.To == c.hosted.started {
			c.hosted.ready, c.hosted.err = true, r.Err
		}
	}
	return out
}

// handleCell starts or stops the cell machine of the add or the drop f of the agent of the
// root p on h, as the host of the controller of a process does:
//   - An add starts the cell machine at the term of the request, and its result is the reply
//     of the machine to Started.
//   - An add of a cell machine of the same term that runs waits for the reply of that machine
//     to Started. The host records that reply itself, so an add finds it also after the wait of
//     the add that started the machine has ended, or after a loss of its reply.
//   - A drop stops the cell machine that runs, and ends at once for one that does not run.
//
// A start of a cell machine that runs at another term fails the case, because the agent of the
// root drops the tenure of an older term before it adds a newer one.
func (h *host) handleCell(p *process, f *machine.Handle) outcome {
	r := f.Request
	ns := r.Scope.Namespace
	id := shard.CellID(ulid.Format(r.Shard.ID()))
	name := address{host: h.name, namespace: ns, role: roleCell, cell: id}.String()
	last := h.cells[name]
	running := last != nil && last.by == p.Epoch() && last.machine != nil && last.machine.Err() == nil &&
		!last.stopped
	switch {
	case r.Call == protocol.Drop && !running:
		return outcome{Event: machine.Handled{Token: f.Token}}
	case r.Call == protocol.Drop:
		last.stopped = true
		return outcome{Event: machine.Handled{Token: f.Token}, Stop: name}
	case r.Call != protocol.Add:
		err := fmt.Errorf("%w: the call %v", protocol.ErrUnsupported, r.Call)
		return outcome{Event: machine.Handled{Err: err, Token: f.Token}}
	case running && last.term == r.Epoch:
		return outcome{Next: func() (machine.Event, bool, bool) {
			switch {
			case last.ready:
				return machine.Handled{Err: last.err, Token: f.Token}, true, true
			case last.machine.Err() != nil:
				return machine.Handled{Err: last.machine.Err(), Token: f.Token}, true, true
			default:
				return nil, false, false
			}
		}}
	}
	scope := shard.Scope{Namespace: ns, Cell: id}
	next := &hosted{scope: scope, term: r.Epoch, by: p.Epoch()}
	h.cells[name] = next
	var reporter telemetry.Reporter = h.reporter
	if h.sys.variant.cellReporter != nil {
		reporter = h.sys.variant.cellReporter(h.sys, h.reporter)
	}
	cfg := cell.Config{
		Clock:       h.clock,
		Rand:        h.rand,
		Reporter:    reporter,
		Provided:    provided,
		Logger:      h.logger,
		Fits:        func(mu *store.Mutation) error { return h.sys.store.Fits(scope, mu) },
		Registry:    h.sys.registry,
		Scope:       scope,
		Operators:   []shard.Identity{operator},
		Node:        h.node,
		Term:        r.Epoch,
		Parallelism: 1,
		Incarnation: r.Operation.Incarnation,
	}
	return outcome{Launch: &sim.Launch[machine.Event, machine.Effect]{
		Machine: func(*sim.Clock) sim.Machine[machine.Event, machine.Effect] {
			m, err := cell.New(&cfg)
			assert.NoError(h.sys.tb, err, "the config of each cell machine must be valid")
			next.machine = m
			var run machine.Machine = m
			if h.sys.variant.cell != nil {
				run = h.sys.variant.cell(h.sys, m)
			}
			return hostedCell{Machine: run, hosted: next}
		},
		Call: func(reply uint64) machine.Event {
			next.started = machine.ReplyToken(reply)
			return machine.Started{Reply: next.started}
		},
		Result: func(_ any, err error) machine.Event { return machine.Handled{Err: delivered(err), Token: f.Token} },
		Name:   name,
		Deps:   []any{h.sys, h, roleCell},
	}}
}

// performAcquire acquires the fence of the token of f in the reference application, and waits
// for the acquisition. The wait ends with the error of its context once the timeout of f has
// passed on the monotonic time of the host, as the context of the call of the driver ends. A
// timer of World.AfterFunc at the deadline makes the world poll the wait at that time. The env
// records each acquisition that the wait reports as done. Under the variant unfenced, the
// performer reports the acquisition as done at once, without a call of the fence.
func performAcquire(w *world, p *process, f *machine.Acquire) outcome {
	s := sim.Dependency[*env](w, p)
	if s.variant.unfenced {
		s.acquisitions = append(s.acquisitions, acquisition{token: f.Fence, records: len(s.app.Records())})
		return outcome{Event: machine.Acquired{Token: f.Token}}
	}
	a := s.app.Acquire(&f.Fence)
	deadline := p.Clock().Monotonic() + f.Timeout
	w.AfterFunc(p, f.Timeout, func() {})
	return outcome{Next: func() (machine.Event, bool, bool) {
		if done, err := a.Result(); done {
			if err == nil {
				s.acquisitions = append(s.acquisitions, acquisition{token: f.Fence, records: len(s.app.Records())})
			}
			return machine.Acquired{Err: err, Token: f.Token}, true, true
		}
		if p.Clock().Monotonic() >= deadline {
			return machine.Acquired{Err: context.DeadlineExceeded, Token: f.Token}, true, true
		}
		return nil, false, false
	}}
}

// performRevoke ends the work of the tenure of the token of f in the reference application.
func performRevoke(w *world, p *process, f *machine.Revoke) outcome {
	sim.Dependency[*env](w, p).app.RevokeShard(&f.Fence)
	return outcome{Event: machine.Revoked{Token: f.Token}}
}

// performPlan runs the placer of f over its input, and then placement.Validate over the plan
// with the constraints of f, on the goroutine of the case. Under the variant unreported, the
// pass drops the violations that placement.Validate returns.
func performPlan(w *world, p *process, f machine.Plan) outcome {
	plan, err := f.Placer.Place(w.Context(), f.Input)
	var violations []placement.Violation
	if err == nil {
		violations, err = placement.Validate(f.Input, plan, f.Constraints)
	}
	if sim.Dependency[*env](w, p).variant.unreported {
		violations = nil
	}
	return outcome{Event: machine.Planned{Err: err, Violations: violations, Plan: plan, Token: f.Token}}
}

// performTimer arms a timer of the world for the duration of f on the clock of the host, which
// passes the Elapsed of f.
func performTimer(w *world, p *process, f machine.Timer) outcome {
	w.After(p, f.Duration, machine.Elapsed{Token: f.Token})
	return outcome{}
}
