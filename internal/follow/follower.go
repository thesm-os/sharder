// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package follow

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=state,event -linecomment -output=follower.string_gen.go

import (
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/fsm"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/resilience"

	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The bounds of the backoff before each retry of a read or a watch. resilience.Backoff
// returns each backoff as a draw of Config.Rand below its ceiling.
const (
	// RetryBase is the ceiling of the backoff before the first retry. The ceiling doubles
	// with each later retry.
	RetryBase = 100 * time.Millisecond
	// RetryMax is the highest ceiling of a backoff.
	RetryMax = 10 * time.Second
)

// Config is the configuration of a [Follower].
type Config struct {
	// Rand is the source of the jitter of each backoff.
	Rand rand.Rand
	// Tokens is the counter of the tokens of the machine. The Follower takes the token of
	// each of its effects from it. The other parts of the machine share it.
	Tokens *machine.Token
	// Scope is the scope of the reads and the watches of the Follower.
	Scope shard.Scope
	// Node selects the placements of one node and the changes that concern the node. The
	// empty Node selects every placement and every change.
	Node shard.NodeID
	// Incarnation is the namespace incarnation that the machine expects. With the zero
	// Incarnation, the Follower takes the incarnation of the scope.
	Incarnation shard.NamespaceIncarnation
}

// Page is one page of a snapshot of the scope. It shares memory with the store, so the
// machine does not modify it.
type Page struct {
	// Placements lists the placements of the page in position order.
	Placements []shard.Placement
	// State is the state of the scope at the generation of the snapshot.
	State store.State
	// First reports that the page is the first page of its snapshot. The machine then
	// discards the pages of an earlier snapshot whose last page has not arrived.
	First bool
	// Last reports that the page is the last page of its snapshot. The snapshot then
	// replaces the state that the machine keeps.
	Last bool
}

// Result is what one event of a [Follower] passes to its machine. An event can pass a page
// of a snapshot or a generation of the watch. An event can also stop the Follower.
//
// The zero Result is the result of an event that passed nothing.
type Result struct {
	// Err is the error that stopped the Follower, or nil.
	Err error
	// Page is the page of a snapshot that the event passed, or nil.
	Page *Page
	// Event is the generation of the watch that the event passed. Its Generation is zero
	// when the event passed no generation. A watch passes only generations above zero.
	Event store.Event
}

// state is the state of a Follower in its lifecycle.
type state uint8

// The states of a Follower.
const (
	// stateIdle is the state before Start.
	stateIdle state = 1 // idle
	// stateReading is the read of the pages of a snapshot.
	stateReading state = 2 // reading
	// stateWatching is the watch of the generations after the generation that the Follower
	// passed last.
	stateWatching state = 3 // watching
	// stateStopped is the state after Stop, and after an error that stopped the Follower.
	stateStopped state = 4 // stopped
)

// event is an event of the lifecycle of a Follower.
type event uint8

// The events of a Follower.
const (
	// eventStart is a call of Start.
	eventStart event = 1 // start
	// eventSnapshot is the last page of a snapshot.
	eventSnapshot event = 2 // snapshot
	// eventCompacted is the end of a watch whose generation the store no longer keeps.
	eventCompacted event = 3 // compacted
	// eventStop is a call of Stop, or an error that stops the Follower.
	eventStop event = 4 // stop
)

// lifecycle is the table of the states of a Follower. eventSnapshot and eventCompacted
// follow the result of an effect of the Follower. When the table has no edge for such an
// event, the Follower stops with the error of fsm.Machine.Fire.
var lifecycle = fsm.Must(fsm.NewBuilder[state, event, struct{}](stateIdle).
	Edge(stateIdle, eventStart, stateReading).
	Edge(stateReading, eventSnapshot, stateWatching).
	Edge(stateWatching, eventCompacted, stateReading).
	Edges([]state{stateIdle, stateReading, stateWatching}, eventStop, stateStopped).
	Terminal(stateStopped).
	Build())

// action is what a Follower sends after an event that it took.
type action uint8

// The actions of a Follower.
const (
	// actNone does not send an effect.
	actNone action = 0
	// actPerform sends the effect that the Follower waits for in its state.
	actPerform action = 1
	// actRetry sends a Timer of the next backoff. The Follower sends the effect of its
	// state again when the Timer has elapsed.
	actRetry action = 2
)

// Follower is the reader of one scope of a machine. It reads the snapshots of the scope in
// pages, and watches the generations after each snapshot. The machine passes each event
// to Step. Step takes the events of the effects of the Follower.
//
// The zero Follower is not usable. Construct one with [New].
//
// # Concurrency
//
// A Follower is not safe for concurrent use. Its machine calls it during Machine.Step.
type Follower struct {
	// rand is the source of the jitter of each backoff.
	rand rand.Rand
	// tokens is the counter of the tokens of the machine.
	tokens *machine.Token
	// lc is the state of the Follower in its lifecycle.
	lc fsm.Machine[state, event, struct{}]
	// id is the scope of the reads and the watches.
	id shard.Scope
	// node is the node that the reads and the watches select, and empty for every node.
	node shard.NodeID
	// page is the token of the next page of the snapshot, and empty for the first page.
	page string
	// generation is the generation of the last snapshot or event that the Follower passed.
	generation epoch.Epoch
	// token is the token of the Read or the Timer that the Follower waits for, and zero
	// when it waits for neither.
	token machine.Token
	// watch is the token of the watch that runs, and zero when no watch runs.
	watch machine.Token
	// attempt counts the failed attempts of the operation. The backoff before the next
	// retry grows with it.
	attempt int
	// inc is the namespace incarnation of the reads and the watches, and zero before the
	// Follower has taken one.
	inc shard.NamespaceIncarnation
}

// New returns a Follower of cfg before Start.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, and for a cfg without Rand, Tokens or
//     the namespace of Scope.
func New(cfg *Config) (*Follower, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	}
	if cfg.Rand == nil {
		return nil, fmt.Errorf("%w: no source of randomness", ErrConfig)
	}
	if cfg.Tokens == nil {
		return nil, fmt.Errorf("%w: no counter of tokens", ErrConfig)
	}
	if cfg.Scope.Namespace == "" {
		return nil, fmt.Errorf("%w: no namespace", ErrConfig)
	}
	return &Follower{
		rand:   cfg.Rand,
		tokens: cfg.Tokens,
		lc:     lifecycle.Start(struct{}{}),
		id:     cfg.Scope,
		node:   cfg.Node,
		inc:    cfg.Incarnation,
	}, nil
}

// Start starts the read of the first snapshot, and appends the read of its first page to
// out. A later call returns out unchanged, and so does a call after Stop, because only the
// state before Start has an edge of its event.
func (f *Follower) Start(out []machine.Effect) []machine.Effect {
	if err := f.fire(eventStart); err != nil {
		return out
	}
	return f.perform(out)
}

// Step takes ev when it is the result of the Read or the Timer that the Follower waits
// for, or an item of the watch that runs. It appends the effects that follow ev to out,
// and the Result reports what ev passed. Step reports false for every other event and
// returns out unchanged. Such an event belongs to another part of the machine. A Follower
// waits for no effect before Start and after it has stopped, so it takes no event then.
func (f *Follower) Step(ev machine.Event, out []machine.Effect) (Result, []machine.Effect, bool) {
	switch ev := ev.(type) {
	case machine.Elapsed:
		if ev.Token == f.token {
			return Result{}, f.perform(out), true
		}
	case machine.PageRead:
		if ev.Token == f.token {
			res, act, err := f.pageRead(&ev)
			return f.after(&res, act, err, out)
		}
	case machine.Watched:
		if ev.Token == f.watch {
			res, act, err := f.watched(&ev)
			return f.after(&res, act, err, out)
		}
	}
	return Result{}, out, false
}

// Stop stops the Follower, and appends the Unwatch of the watch that runs to out. A
// stopped Follower takes no event, and a later call returns out unchanged.
func (f *Follower) Stop(out []machine.Effect) []machine.Effect {
	//dokimi:mutate-skip ror-false: a stopped Follower has no token and no watch, so the rest of Stop does not change it
	if err := f.fire(eventStop); err != nil {
		return out
	}
	f.token = 0
	return f.unwatch(out)
}

// after completes an event that the Follower took with the Result res. It stops the
// Follower with err, the error of the event or of its edge in the lifecycle, and sends
// what act asks for otherwise.
func (f *Follower) after(res *Result, act action, err error, out []machine.Effect) (Result, []machine.Effect, bool) {
	if err != nil {
		return f.stop(err), out, true
	}
	switch act {
	case actNone:
	case actPerform:
		out = f.perform(out)
	case actRetry:
		out = f.retry(out)
	}
	return *res, out, true
}

// pageRead takes one page of a snapshot and passes it to the machine. It reads the next
// page, or starts the watch after the last page.
func (f *Follower) pageRead(ev *machine.PageRead) (Result, action, error) {
	if errors.Is(ev.Err, store.ErrCompacted) {
		f.page = ""
		return Result{}, actRetry, nil
	}
	if ev.Err != nil {
		act, err := failed(fmt.Errorf("follow: a page of %v: %w", f.id, ev.Err))
		return Result{}, act, err
	}
	p := &Page{Placements: ev.Placements, State: ev.State, First: f.page == "", Last: ev.Next == ""}
	if f.inc.IsZero() {
		f.inc = ev.State.Head.Incarnation
	}
	f.page, f.attempt = ev.Next, 0
	if !p.Last {
		return Result{Page: p}, actPerform, nil
	}
	f.generation = ev.State.Head.Generation
	return Result{Page: p}, actPerform, f.fire(eventSnapshot)
}

// watched takes one item of the watch. It passes a later generation to the machine, and
// starts a new snapshot or resumes the watch after an error. It starts the watch again
// with the first incarnation that an item gives a Follower without one.
func (f *Follower) watched(ev *machine.Watched) (Result, action, error) {
	if ev.Err != nil {
		f.watch = 0
		if errors.Is(ev.Err, store.ErrCompacted) {
			return Result{}, actRetry, f.fire(eventCompacted)
		}
		act, err := failed(fmt.Errorf("follow: the watch of %v: %w", f.id, ev.Err))
		return Result{}, act, err
	}
	if ev.Event.Generation <= f.generation {
		return Result{}, actNone, nil
	}
	f.generation, f.attempt = ev.Event.Generation, 0
	if inc := ev.Event.Mutation.ID.Incarnation; f.inc.IsZero() && !inc.IsZero() {
		f.inc = inc
		return Result{Event: ev.Event}, actPerform, nil
	}
	return Result{Event: ev.Event}, actNone, nil
}

// perform takes a new token from the counter of the machine, and appends the effect that
// the Follower waits for in its state to out with that token. A new watch replaces the
// watch that runs.
func (f *Follower) perform(out []machine.Effect) []machine.Effect {
	if f.lc.State() == stateReading {
		f.token = f.next()
		return append(out, machine.Read{
			Scope:       f.id,
			Query:       store.Query{Node: f.node},
			Page:        page.Page{Token: f.page},
			Token:       f.token,
			Incarnation: f.inc,
		})
	}
	out = f.unwatch(out)
	f.token, f.watch = 0, f.next()
	return append(
		out,
		machine.Watch{Scope: f.id, Node: f.node, Token: f.watch, From: f.generation, Incarnation: f.inc},
	)
}

// unwatch appends the Unwatch of the watch that runs to out, and returns out unchanged
// when no watch runs.
func (f *Follower) unwatch(out []machine.Effect) []machine.Effect {
	if f.watch != 0 {
		out = append(out, machine.Unwatch{Token: f.watch})
	}
	f.watch = 0
	return out
}

// retry appends a Timer of the next backoff to out. The Follower performs the next
// operation when the Timer has elapsed.
func (f *Follower) retry(out []machine.Effect) []machine.Effect {
	f.attempt++
	f.token = f.next()
	return append(
		out,
		machine.Timer{Token: f.token, Duration: resilience.Backoff(f.rand, f.attempt, RetryBase, RetryMax)},
	)
}

// stop stops the Follower with err, and returns the Result that reports err. The error of
// the edge of eventStop joins err. The Follower has no watch when an error stops it. It
// reads without a watch, and an error of the watch ends the watch.
func (f *Follower) stop(err error) Result {
	fail := f.fire(eventStop)
	f.token = 0
	return Result{Err: errors.Join(err, fail)}
}

// fire delivers ev to the lifecycle of f. When the lifecycle refuses ev, fire returns the
// error of fsm.Machine.Fire with the event, the scope and the state of f.
func (f *Follower) fire(ev event) error {
	if st, err := f.lc.Fire(ev); err != nil {
		return fmt.Errorf("follow: the event %v of %v in the state %v: %w", ev, f.id, st, err)
	}
	return nil
}

// next returns the next token of the machine.
func (f *Follower) next() machine.Token {
	*f.tokens++
	return *f.tokens
}

// failed returns actRetry for an err that errs.Retryable reports as transient, and err
// otherwise, which stops the Follower.
func failed(err error) (action, error) {
	if errs.Retryable(err) {
		return actRetry, nil
	}
	return actNone, err
}
