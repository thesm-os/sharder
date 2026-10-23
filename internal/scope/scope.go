// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package scope

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/resilience"
	"go.thesmos.sh/core/telemetry"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The bounds of the backoff before each retry of a read or a commit. resilience.Backoff
// returns each backoff as a draw of Config.Rand below its ceiling.
const (
	// RetryBase is the ceiling of the backoff before the first retry. The ceiling doubles
	// with each later retry.
	RetryBase = 100 * time.Millisecond
	// RetryMax is the highest ceiling of a backoff.
	RetryMax = 10 * time.Second
)

// Acquisition returns the namespace incarnation of the writer of a scope and the mutation
// that acquires the scope, for the header head of one attempt of the acquisition. The
// mutation contains the record of the controller in Cell. For a head without an
// incarnation, the mutation creates the scope. The root of a new namespace then also puts
// the mapping and the spec of the namespace in it, and returns the incarnation that it
// generated. The root of an existing namespace returns the incarnation of head. The
// controller of a cell returns the incarnation that the root gave it with the cell. Each
// call returns a new mutation, which the Scope keeps and gives its commit ID.
type Acquisition func(head store.Head) (shard.NamespaceIncarnation, *store.Mutation)

// Config is the configuration of a [Scope].
type Config struct {
	// Rand is the source of the jitter of each backoff.
	Rand rand.Rand
	// Unknown counts the attempts of commits that fail with an unknown outcome. The machine
	// binds the counter to its scope.
	Unknown telemetry.Counter
	// Acquisition returns the incarnation and the mutation of each attempt of the
	// acquisition.
	Acquisition Acquisition
	// Tokens is the counter of the tokens of the machine. The Scope takes the token of each
	// of its effects from it, as the other parts of the machine do.
	Tokens *machine.Token
	// Scope is the scope that the Scope writes.
	Scope shard.Scope
	// Term is the fence epoch of the writer: the epoch that the root granted to the
	// controller of a cell, or the term that the election granted to the root. A machine
	// creates one Scope per term, because the commit IDs of each Scope start at one.
	Term epoch.Epoch
}

// Snapshot is the state of a scope and its placements at the generation of the
// acquisition. It shares memory with the store, so the machine does not modify it.
type Snapshot struct {
	// Placements lists every placement of the scope in position order.
	Placements []shard.Placement
	// State is the state of the scope at the generation of the acquisition.
	State store.State
}

// Result is what one event completed in a [Scope], for its machine. An event completes
// the acquisition, applies one commit, or stops the scope, and most events complete none
// of them.
//
// The zero Result is the result of an event that completed nothing.
type Result struct {
	// Err is the error that stopped the scope, or nil.
	Err error
	// Snapshot is the snapshot of the acquisition, when the event completed it, and nil
	// otherwise.
	Snapshot *Snapshot
	// Commit is the ID of the commit that the event applied, or the zero ID.
	Commit store.CommitID
	// Generation is the generation that the commit created.
	Generation epoch.Epoch
}

// state is the state of a Scope in its lifecycle.
//
//dokimi:enum text=comment
type state uint8

// The states of a Scope.
const (
	// stateIdle is the state before Acquire.
	stateIdle state = 1 // idle
	// stateHeading is the read of the header of an attempt of the acquisition.
	stateHeading state = 2 // heading
	// stateAcquiring is the commit of the mutation of an attempt of the acquisition.
	stateAcquiring state = 3 // acquiring
	// stateSettling is the read of the receipt of the commit of the acquisition, after a
	// retry of the commit found another generation.
	stateSettling state = 4 // settling
	// stateReading is the read of the pages of the snapshot.
	stateReading state = 5 // reading
	// stateReady is the state without a commit in flight.
	stateReady state = 6 // ready
	// stateCommitting is a commit of a mutation of the machine.
	stateCommitting state = 7 // committing
	// stateChecking is the read of the receipt of a commit of the machine, after a retry of
	// the commit found another generation.
	stateChecking state = 8 // checking
	// stateStopped is the state after an error has stopped the scope.
	stateStopped state = 9 // stopped
)

// event is an event of the lifecycle of a Scope.
//
//dokimi:enum text=comment
type event uint8

// The events of a Scope.
const (
	// eventAcquire is a call of Acquire.
	eventAcquire event = 1 // acquire
	// eventHeader is a header at which an attempt of the acquisition commits.
	eventHeader event = 2 // header
	// eventApplied is a commit that has applied while no mutation waits.
	eventApplied event = 3 // applied
	// eventChained is a commit that has applied while a mutation waits, so the Scope
	// commits the mutation next.
	eventChained event = 4 // chained
	// eventUnsettled is a retry of a commit with an unknown outcome that found another
	// generation, so the Scope reads the receipt of the commit.
	eventUnsettled event = 5 // unsettled
	// eventLost is an attempt of the acquisition whose commit did not apply.
	eventLost event = 6 // lost
	// eventSnapshot is the last page of the snapshot.
	eventSnapshot event = 7 // snapshot
	// eventCommit is a call of Commit.
	eventCommit event = 8 // commit
	// eventFail is an error that stops the scope.
	eventFail event = 9 // fail
)

// lifecycle is the table of the states of a Scope. Each event other than eventAcquire and
// eventCommit follows the result of an effect of the Scope. When the table has no edge for
// such an event, the Scope stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventAcquire, stateHeading).
	Edge(stateHeading, eventHeader, stateAcquiring).
	Edge(stateAcquiring, eventApplied, stateReading).
	Edge(stateAcquiring, eventUnsettled, stateSettling).
	Edge(stateAcquiring, eventLost, stateHeading).
	Edge(stateSettling, eventApplied, stateReading).
	Edge(stateSettling, eventLost, stateHeading).
	Edge(stateReading, eventSnapshot, stateReady).
	Edge(stateReady, eventCommit, stateCommitting).
	Edge(stateCommitting, eventCommit, stateCommitting).
	Edge(stateCommitting, eventApplied, stateReady).
	Edge(stateCommitting, eventChained, stateCommitting).
	Edge(stateCommitting, eventUnsettled, stateChecking).
	Edge(stateChecking, eventCommit, stateChecking).
	Edge(stateChecking, eventApplied, stateReady).
	Edge(stateChecking, eventChained, stateCommitting).
	Edges([]state{stateHeading, stateAcquiring, stateSettling, stateReading, stateCommitting, stateChecking},
		eventFail, stateStopped).
	Terminal(stateStopped).
	Build())

// action is what a Scope sends after an event that it took.
type action uint8

// The actions of a Scope.
const (
	// actNone does not send an effect.
	actNone action = 0
	// actPerform sends the effect that the Scope waits for in its state.
	actPerform action = 1
	// actRetry sends a Timer of the next backoff. The Scope sends the effect of its state
	// again when the Timer has elapsed.
	actRetry action = 2
)

// Scope is the writer of one scope of a machine. It acquires the scope and commits the
// mutations of the machine one at a time. It settles the outcome of each commit before it
// sends the next one. The machine passes each event to Step, which takes the events of
// the effects of the Scope.
//
// The Scope keeps each mutation until its commit has applied or has failed, and keeps the
// pages of the snapshot until it reports the snapshot. A stopped Scope releases both.
//
// The zero Scope is not usable. Construct one with [New].
//
// # Concurrency
//
// A Scope is not safe for concurrent use. Its machine calls it during Machine.Step.
type Scope struct {
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// unknowns counts the attempts of commits that fail with an unknown outcome.
	unknowns telemetry.Counter
	// acquisition returns the incarnation and the mutation of each attempt of the
	// acquisition.
	acquisition Acquisition
	// tokens is the counter of the tokens of the machine.
	tokens *machine.Token
	// pending is the mutation of the commit in flight, and nil without one.
	pending *store.Mutation
	// lc is the state of the Scope in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// id is the scope.
	id shard.Scope
	// page is the token of the next page of the snapshot, and empty for the first page.
	page string
	// queue lists the mutations that wait for the commit in flight, in the order of their
	// calls.
	queue []*store.Mutation
	// confirm lists the IDs of the commits that applied after the last commit that
	// confirmed receipts.
	confirm []store.CommitID
	// snapshot is the part of the snapshot that the pages read so far contain.
	snapshot Snapshot
	// term is the fence epoch of the writer.
	term epoch.Epoch
	// generation is the generation that the last applied commit created.
	generation epoch.Epoch
	// expect is the generation that the commit in flight expects.
	expect epoch.Epoch
	// seq is the Seq of the last commit ID that the Scope gave.
	seq uint64
	// token is the token of the effect or the Timer that the Scope waits for, and zero
	// when it waits for none.
	token machine.Token
	// attempt counts the failed attempts of the effect in flight, for its backoff.
	attempt int
	// inc is the namespace incarnation of the writer.
	inc shard.NamespaceIncarnation
	// unknown reports that an attempt of the commit in flight returned an unknown outcome.
	unknown bool
	// creation reports that the header of the acquisition had no incarnation, so the
	// commit of the acquisition creates the scope.
	creation bool
}

// New returns a Scope of cfg before its acquisition.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Rand, Unknown,
//     Acquisition, Tokens, the namespace of Scope, or Term.
func New(cfg *Config) (*Scope, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if cfg.Rand == nil {
		return nil, fmt.Errorf("%w: no source of randomness", ErrConfig)
	}
	if cfg.Unknown == nil {
		return nil, fmt.Errorf("%w: no counter of unknown outcomes", ErrConfig)
	}
	if cfg.Acquisition == nil {
		return nil, fmt.Errorf("%w: no acquisition", ErrConfig)
	}
	if cfg.Tokens == nil {
		return nil, fmt.Errorf("%w: no counter of tokens", ErrConfig)
	}
	if cfg.Scope.Namespace == "" {
		return nil, fmt.Errorf("%w: no namespace", ErrConfig)
	}
	if cfg.Term == epoch.Zero {
		return nil, fmt.Errorf("%w: no term", ErrConfig)
	}
	return &Scope{
		rand:        cfg.Rand,
		unknowns:    cfg.Unknown,
		acquisition: cfg.Acquisition,
		tokens:      cfg.Tokens,
		lc:          lifecycle.Start(struct{}{}),
		id:          cfg.Scope,
		term:        cfg.Term,
	}, nil
}

// Acquire starts the acquisition of the scope, and appends the read of its header to out.
// A later call returns out unchanged, because only the state before the acquisition has an
// edge of its event.
func (s *Scope) Acquire(out []machine.Effect) []machine.Effect {
	if err := s.fire(eventAcquire); err != nil {
		return out
	}
	return s.perform(out)
}

// Commit gives m the next commit ID of the term, and commits m after the mutations of the
// earlier calls. It appends the commit to out when no commit is in flight, and returns the
// ID of m. Commit sets the ID and the Confirm of m, so the machine does not modify m after
// the call. Step reports the commit in a Result once it has applied.
//
// Error modes:
//   - an error that wraps ErrNotAcquired and fsm.ErrRejected before the acquisition has
//     completed, and after the scope has stopped.
func (s *Scope) Commit(m *store.Mutation, out []machine.Effect) (store.CommitID, []machine.Effect, error) {
	from := s.lc.State()
	if err := s.fire(eventCommit); err != nil {
		return store.CommitID{}, out, fmt.Errorf("%w: %w", ErrNotAcquired, err)
	}
	s.seq++
	m.ID = store.CommitID{Incarnation: s.inc, Term: s.term, Seq: s.seq}
	if from != stateReady {
		s.queue = append(s.queue, m)
		return m.ID, out, nil
	}
	s.load(m, s.generation)
	return m.ID, s.perform(out), nil
}

// Step takes ev when it is the result of the effect or the Timer that the Scope waits for,
// and appends the effects that follow ev to out. The Result reports what ev completed.
// Step reports false for every other event and returns out unchanged. Such an event
// belongs to another part of the machine.
func (s *Scope) Step(ev machine.Event, out []machine.Effect) (Result, []machine.Effect, bool) {
	switch ev := ev.(type) {
	case machine.Elapsed:
		if ev.Token == s.token {
			return Result{}, s.perform(out), true
		}
	case machine.HeadRead:
		if ev.Token == s.token {
			act, err := s.headRead(&ev)
			return s.after(&Result{}, act, err, out)
		}
	case machine.Committed:
		if ev.Token == s.token {
			res, act, err := s.committed(&ev)
			return s.after(&res, act, err, out)
		}
	case machine.OutcomeRead:
		if ev.Token == s.token {
			res, act, err := s.outcomeRead(&ev)
			return s.after(&res, act, err, out)
		}
	case machine.PageRead:
		if ev.Token == s.token {
			res, act, err := s.pageRead(&ev)
			return s.after(&res, act, err, out)
		}
	}
	return Result{}, out, false
}

// after completes an event that the Scope took with the Result res. It stops the Scope with
// err, the error of the event or of its edge in the lifecycle, and sends what act asks for
// otherwise.
func (s *Scope) after(res *Result, act action, err error, out []machine.Effect) (Result, []machine.Effect, bool) {
	if err != nil {
		return s.stop(err), out, true
	}
	switch act {
	case actNone:
	case actPerform:
		out = s.perform(out)
	case actRetry:
		out = s.retry(out)
	}
	return *res, out, true
}

// headRead takes the header of an attempt of the acquisition. It refuses a header of a newer
// schema, and loads the mutation of the acquisition at the generation of the header
// otherwise.
func (s *Scope) headRead(ev *machine.HeadRead) (action, error) {
	if ev.Err != nil {
		return failed(fmt.Errorf("scope: the header of %v: %w", s.id, ev.Err))
	}
	if ev.Head.Schema > store.Schema {
		return actNone, fmt.Errorf("%w: %v has the schema %d, and this release writes the schema %d",
			ErrSchema, s.id, ev.Head.Schema, store.Schema)
	}
	inc, m := s.acquisition(ev.Head)
	s.inc = inc
	s.creation = ev.Head.Incarnation.IsZero()
	s.seq++
	m.ID = store.CommitID{Incarnation: inc, Term: s.term, Seq: s.seq}
	s.load(m, ev.Head.Generation)
	return actPerform, s.fire(eventHeader)
}

// committed takes the result of an attempt of the commit in flight. It counts an attempt
// with an unknown outcome in Config.Unknown.
func (s *Scope) committed(ev *machine.Committed) (Result, action, error) {
	if ev.Err == nil {
		return s.applied(ev.Generation)
	}
	if errors.Is(ev.Err, version.ErrOutcomeUnknown) {
		s.unknown = true
		s.unknowns.Add(context.Background(), 1)
		return Result{}, actRetry, nil
	}
	mismatch := errors.Is(ev.Err, version.ErrMismatch)
	switch {
	case mismatch && s.unknown:
		s.attempt = 0
		return Result{}, actPerform, s.fire(eventUnsettled)
	case s.lc.State() == stateAcquiring && (mismatch || s.creation && errors.Is(ev.Err, shard.ErrIncarnation)):
		return s.reacquire()
	case mismatch:
		return Result{}, actNone, fmt.Errorf("%w: commit %v to %v: %w", ErrChanged, s.pending.ID, s.id, ev.Err)
	}
	return Result{}, actNone, fmt.Errorf("scope: commit %v to %v: %w", s.pending.ID, s.id, ev.Err)
}

// outcomeRead takes the receipt of a commit whose outcome was unknown. The Scope reads the
// receipt when a retry of the commit has found another generation.
func (s *Scope) outcomeRead(ev *machine.OutcomeRead) (Result, action, error) {
	if ev.Err != nil {
		act, err := failed(fmt.Errorf("scope: the receipt of commit %v in %v: %w", s.pending.ID, s.id, ev.Err))
		return Result{}, act, err
	}
	if ev.Found {
		return s.applied(ev.Receipt.Generation)
	}
	if s.lc.State() == stateSettling {
		return s.reacquire()
	}
	return Result{}, actNone, fmt.Errorf("%w: commit %v to %v has no receipt after another commit moved the generation",
		ErrChanged, s.pending.ID, s.id)
}

// pageRead takes one page of the snapshot. It reads the next page, or completes the
// acquisition with the snapshot after the last page.
func (s *Scope) pageRead(ev *machine.PageRead) (Result, action, error) {
	if errors.Is(ev.Err, store.ErrCompacted) {
		s.page, s.snapshot = "", Snapshot{}
		return Result{}, actRetry, nil
	}
	if ev.Err != nil {
		act, err := failed(fmt.Errorf("scope: a page of %v: %w", s.id, ev.Err))
		return Result{}, act, err
	}
	if s.page == "" {
		if ev.State.Head.Generation != s.generation {
			return Result{}, actNone, fmt.Errorf(
				"%w: %v is at the generation %d after the acquisition created the generation %d",
				ErrChanged, s.id, ev.State.Head.Generation, s.generation)
		}
		s.snapshot.State = ev.State
	}
	s.snapshot.Placements = append(s.snapshot.Placements, ev.Placements...)
	s.attempt = 0
	if ev.Next != "" {
		s.page = ev.Next
		return Result{}, actPerform, nil
	}
	snap := s.snapshot
	s.snapshot = Snapshot{}
	s.token = 0
	return Result{Snapshot: &snap}, actNone, s.fire(eventSnapshot)
}

// applied completes the commit in flight. The commit created the generation gen. After the
// commit of the acquisition, applied reads the snapshot. After any other commit, it
// reports the commit, and loads the next waiting mutation when one waits.
func (s *Scope) applied(gen epoch.Epoch) (Result, action, error) {
	id := s.pending.ID
	s.generation = gen
	s.confirm = append(s.confirm, id)
	s.pending, s.token = nil, 0
	if st := s.lc.State(); st == stateAcquiring || st == stateSettling {
		s.attempt = 0
		return Result{}, actPerform, s.fire(eventApplied)
	}
	res := Result{Commit: id, Generation: gen}
	if len(s.queue) == 0 {
		return res, actNone, s.fire(eventApplied)
	}
	m := s.queue[0]
	s.queue = slices.Delete(s.queue, 0, 1)
	s.load(m, s.generation)
	return res, actPerform, s.fire(eventChained)
}

// load makes m the mutation of the commit in flight, which expects the generation expect.
// It moves the IDs of the commits that applied since the last confirmation to m.Confirm, so
// the commit confirms their receipts.
func (s *Scope) load(m *store.Mutation, expect epoch.Epoch) {
	m.Confirm, s.confirm = s.confirm, nil
	s.pending, s.expect = m, expect
	s.unknown = false
	s.attempt = 0
}

// reacquire starts the next attempt of the acquisition after an attempt whose commit did
// not apply. The next attempt reads the header again.
func (s *Scope) reacquire() (Result, action, error) {
	s.pending = nil
	s.attempt = 0
	return Result{}, actPerform, s.fire(eventLost)
}

// perform takes a new token, and appends the effect that the Scope waits for in its state
// to out with that token.
func (s *Scope) perform(out []machine.Effect) []machine.Effect {
	s.token = s.next()
	switch s.lc.State() {
	case stateHeading:
		return append(out, machine.Head{Scope: s.id, Token: s.token})
	case stateAcquiring, stateCommitting:
		return append(out, machine.Commit{
			Mutation:    s.pending,
			Scope:       s.id,
			Token:       s.token,
			Term:        s.term,
			Expect:      s.expect,
			Incarnation: s.inc,
		})
	case stateSettling, stateChecking:
		return append(out, machine.Outcome{Scope: s.id, Token: s.token, ID: s.pending.ID})
	default:
		return append(
			out,
			machine.Read{Scope: s.id, Page: page.Page{Token: s.page}, Token: s.token, Incarnation: s.inc},
		)
	}
}

// retry appends a Timer of the next backoff to out. The effect of the state of the Scope
// runs again when the Timer has elapsed.
func (s *Scope) retry(out []machine.Effect) []machine.Effect {
	s.attempt++
	s.token = s.next()
	return append(
		out,
		machine.Timer{Token: s.token, Duration: resilience.Backoff(s.rand, s.attempt, RetryBase, RetryMax)},
	)
}

// stop stops the scope with err, and returns the Result that reports err. It releases the
// mutations and the pages that the Scope keeps. A stopped Scope does not take an event
// again. The error of the edge of eventFail joins err.
func (s *Scope) stop(err error) Result {
	fail := s.fire(eventFail)
	s.token = 0
	s.pending, s.queue, s.confirm = nil, nil, nil
	s.snapshot = Snapshot{}
	return Result{Err: errors.Join(err, fail)}
}

// fire delivers ev to the lifecycle of s. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the scope and the state of s.
func (s *Scope) fire(ev event) error {
	if st, err := s.lc.Fire(ev); err != nil {
		return fmt.Errorf("scope: the event %v of %v in the state %v: %w", ev, s.id, st, err)
	}
	return nil
}

// next returns the next token of the machine.
func (s *Scope) next() machine.Token {
	*s.tokens++
	return *s.tokens
}

// failed returns actRetry for an err that errs.Retryable reports as transient, and err
// otherwise, which stops the Scope.
func failed(err error) (action, error) {
	if errs.Retryable(err) {
		return actRetry, nil
	}
	return actNone, err
}
