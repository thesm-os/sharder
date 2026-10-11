// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest

//go:generate go run golang.org/x/tools/cmd/stringer@v0.52.0 -type=Loss -linecomment -output=store.string_gen.go

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/coretest/epochtest"
	"go.thesmos.sh/core/crypto"
	"go.thesmos.sh/core/crypto/sha256"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/version"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/streamtest"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// The names in the records of the suite.
const (
	// namespace is the namespace of every scope of the suite.
	namespace shard.Namespace = "storetest"
	// cell is the cell of the scope that the laws commit to.
	cell shard.CellID = "cell-1"
	// fencedCell is the prefix of the cells of the scopes of the fence laws.
	fencedCell = "fenced-"
	// protocol is the handoff protocol of the transitions of the suite.
	protocol shard.StrategyName = "storetest"
	// partitioner is the partitioner of the mapping of the suite.
	partitioner shard.StrategyName = "fixed-slots"
	// caller is the identity that requests the drains and the disruptions, and submits the
	// proposals, of the suite.
	caller shard.Identity = "spiffe://storetest/operator"
	// controller is the address of the controller in the controller records of the suite.
	controller = "controller.storetest:7400"
)

// The nodes of the replicas and the endpoints of the suite.
const (
	nodeA shard.NodeID = "node-a"
	nodeB shard.NodeID = "node-b"
	nodeC shard.NodeID = "node-c"
)

// The names of the calls in the histories of the suite, and of the actions of the machine
// of commits.
const (
	// opHead is a call of Head.
	opHead = "head"
	// opRead is a call of Read, with the header of the state that it returns.
	opRead = "read"
	// opOutcome is a call of Outcome.
	opOutcome = "outcome"
	// opCommit is a call of Commit.
	opCommit = "commit"
	// opCommitAtHead is the action of a call of Head and a call of Commit at the generation
	// that Head returned.
	opCommitAtHead = "commit at the head"
	// opLose is the action that makes the next commit fail.
	opLose = "lose"
	// opLead is a call of Campaign that returned a term.
	opLead = "lead"
	// opResign is a call of Resign.
	opResign = "resign"
)

// The labels of the outcomes of the commits of the machine of commits, which each case of
// the machine counts.
const (
	// labelApplied counts a case with a commit that returned the generation after the one
	// that it expects.
	labelApplied = "a commit returns the generation after the one that it expects"
	// labelReplayed counts a case with a commit that returned the generation of its
	// receipt, other than the generation after the one that it expects.
	labelReplayed = "a commit returns an earlier generation of its receipt"
	// labelIncarnation counts a case with a commit that returned shard.ErrIncarnation.
	labelIncarnation = "a commit returns ErrIncarnation"
	// labelFenced counts a case with a commit that returned epoch.ErrFenced.
	labelFenced = "a commit returns ErrFenced"
	// labelReused counts a case with a commit that returned store.ErrReused.
	labelReused = "a commit returns ErrReused"
	// labelMismatch counts a case with a commit that returned version.ErrMismatch.
	labelMismatch = "a commit returns ErrMismatch"
	// labelUnknown counts a case with a commit whose outcome is unknown.
	labelUnknown = "a commit returns ErrOutcomeUnknown"
)

// The least shares of the cases of the machine of commits that count each label. Each
// outcome occurs in about three times as many cases of a correct store, so a run decides
// each requirement after its first 100 cases.
const (
	// shareHalf is the least share of the outcomes that occur in about half of the cases.
	// A commit that applies and a commit of another incarnation are such outcomes.
	shareHalf = 0.15
	// shareThird is the least share of the outcomes that occur in about a third of the
	// cases. A commit at another generation, a fenced commit and a lost commit are such
	// outcomes.
	shareThird = 0.1
	// shareQuarter is the least share of the outcomes that occur in about a quarter of the
	// cases. A reused commit ID and a retry that returns the generation of its receipt are
	// such outcomes.
	shareQuarter = 0.05
)

// The sizes of the suite.
const (
	// shardCount is the number of shards of the generated placements and transitions.
	shardCount = 6
	// proposalCount is the number of IDs of the generated proposals.
	proposalCount = 3
	// disruptionCount is the number of IDs of the generated disruptions.
	disruptionCount = 3
	// drainCount is the number of keys of the generated drains.
	drainCount = 4
	// maxBatch is the most entries of one list of a generated mutation, such as its puts.
	maxBatch = 3
	// maxCommits is the most mutations that one case of a law commits.
	maxCommits = 5
	// maxLimit is the largest limit of a page of the law of pages.
	maxLimit = 4
	// rangeStep is the distance between the starts of the ranges of two neighbouring
	// shards.
	rangeStep = 0x20
	// seqs is the number of commit numbers of each writer of the machine of commits.
	seqs = 3
	// variants is the number of mutations of the machine of commits that share one commit
	// ID.
	variants = 2
	// confirming is the variant of a mutation of the machine of commits that confirms the
	// commit of its writer before it.
	confirming = 1
	// expects is the highest generation that a commit of the machine of commits expects.
	expects = 4
	// maxLost is the most commits that the machine of commits loses in one case. A lost
	// commit remains pending until the end of its history, and the check of
	// linearizability can linearize it at any later point or never. The steps of the
	// check grow exponentially with the number of lost commits.
	maxLost = 2
	// candidates is the number of clients of the law of terms.
	candidates = 3
	// rounds is the number of terms that each client of the law of terms leads.
	rounds = 3
	// maxFitsPlacements is the most placements of a mutation of the law of Fits below the
	// limit of the store.
	maxFitsPlacements = 32
	// maxFitsBytes is the most bytes of the spec of a mutation of the law of Fits below the
	// limit of the store.
	maxFitsBytes = 8192
	// fitsPlacementSlack is how many placements a mutation of the law of Fits puts beyond
	// the limit of the store.
	fitsPlacementSlack = 2
	// fitsByteSlack is how many bytes the spec of a mutation of the law of Fits has beyond
	// the limit of the store.
	fitsByteSlack = 64
)

// The first bytes of the identifiers of the suite, one per kind of identifier, so that no
// two kinds share an identifier.
const (
	tagIncarnation byte = 0x1a
	tagShard       byte = 0x5d
	tagProposal    byte = 0x9c
	tagDisruption  byte = 0xd1
	tagFits        byte = 0xf1
)

// The terms and the phases of the suite.
const (
	// lawTerm is the term of the writers of the laws apart from the machine of commits.
	lawTerm epoch.Epoch = 1
	// highTerm is the term of the commit that raises the watermark of the law of terms.
	highTerm epoch.Epoch = 7
	// invalidPhase is a phase that is not Valid, so a transition with it does not encode.
	invalidPhase = shard.PhaseAborted + 1
)

// waitFor bounds each wait of the suite for an event, a term or the end of a watch that a
// correct store delivers at once.
const waitFor = 10 * time.Second

// The identities and the instants of the suite.
var (
	// incA is the namespace incarnation of the writers of the laws.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the writers of the laws of another incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// scope is the scope that the laws commit to.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// origin is the first instant of the records of the suite.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
	// rejections lists the definite rejections of a commit after its encoding, in the
	// order of the checks of Commit, each with the label of its outcome.
	rejections = [...]struct {
		err   error
		label string
	}{
		{err: shard.ErrIncarnation, label: labelIncarnation},
		{err: epoch.ErrFenced, label: labelFenced},
		{err: store.ErrReused, label: labelReused},
		{err: version.ErrMismatch, label: labelMismatch},
	}
	// modelSeed is the seed of the hashes of the states of the machine of commits.
	modelSeed = maphash.MakeSeed()
)

// The generators of the parts of the records of the suite.
var (
	// instants generates the instants of the records: whole seconds in the hour after
	// origin, in UTC.
	instants = prop.Integer(0, 3600).Map(func(s int) time.Time {
		return origin.Add(time.Duration(s) * time.Second)
	})
	// nodes generates the node of a replica, an endpoint or a disruption.
	nodes = prop.SampledFrom(nodeA, nodeB, nodeC)
	// replicaSets generates the replicas of a placement: up to three nodes, the first in
	// the role primary and the others in the role secondary.
	replicaSets = prop.List(nodes, prop.Unique(), prop.MaxSize(3)).Map(func(ns []shard.NodeID) []shard.Replica {
		rs := make([]shard.Replica, len(ns))
		for i, n := range ns {
			rs[i] = shard.Replica{Node: n, Role: shard.RoleSecondary}
		}
		if len(rs) > 0 {
			rs[0].Role = shard.RolePrimary
		}
		return rs
	})
	// endpoints generates the endpoints of a transition: no endpoint, or a node in the
	// role primary or secondary.
	endpoints = prop.Composite(func(c *prop.Case) store.Endpoint {
		node := c.Draw(prop.SampledFrom[shard.NodeID]("", nodeA, nodeB, nodeC), "node")
		if node == "" {
			return store.Endpoint{}
		}
		return store.Endpoint{
			Node:        node,
			Incarnation: 1,
			Role:        c.Draw(prop.SampledFrom(shard.RolePrimary, shard.RoleSecondary), "role"),
		}
	})
	// specs generates the spec of a mutation: none, or one of two specs.
	specs = prop.SampledFrom[[]byte](nil, []byte(`{"version":1}`), []byte(`{"version":2}`))
	// shardSubsets generates the indexes of the shards of one list of a mutation.
	shardSubsets = prop.List(prop.Integer(0, shardCount-1), prop.Unique(), prop.MaxSize(maxBatch))
	// proposalSubsets generates the indexes of the proposals of one list of a mutation.
	proposalSubsets = prop.List(prop.Integer(0, proposalCount-1), prop.Unique(), prop.MaxSize(maxBatch))
	// disruptionSubsets generates the indexes of the disruptions of one list of a mutation.
	disruptionSubsets = prop.List(prop.Integer(0, disruptionCount-1), prop.Unique(), prop.MaxSize(maxBatch))
	// drainSubsets generates the indexes of the drain keys of one list of a mutation.
	drainSubsets = prop.List(prop.Integer(0, drainCount-1), prop.Unique(), prop.MaxSize(maxBatch))
)

// Loss is the point at which a fault that [WithLostCommits] injects interrupts a commit.
//
// The zero Loss is not Valid.
//
// # Concurrency
//
// A Loss is a comparable value.
type Loss uint8

// The points at which a fault interrupts a commit.
const (
	// LoseRequest loses a commit before the store applies it, so the commit does not apply.
	LoseRequest Loss = 1 // request
	// LoseReply loses the reply of a commit after the store applies it, so the commit and
	// its receipt remain.
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

// config is the result of the Options of one run of AssertStore.
type config struct {
	// reopen opens the storage behind a store again, or is nil.
	reopen func(tb assert.TB, s store.Store) store.Store
	// lose makes the next commit to a store fail, or is nil.
	lose func(tb assert.TB, s store.Store, l Loss)
	// del deletes a scope from the storage behind a store, or is nil.
	del func(tb assert.TB, s store.Store, sc shard.Scope)
	// compact discards the generations of the scopes of a store below their current ones,
	// or is nil.
	compact func(tb assert.TB, s store.Store)
}

// Option configures a run of [AssertStore]. Each Option adds the laws that its docblock
// lists.
type Option func(*config)

// WithReopen gives the suite a way to open the storage behind s again, as a process does
// after a restart. reopen closes s, or lets it fail as a crash does, and returns a new
// store over the same storage. The suite closes the store that reopen returns, and does
// not close s. It then checks that every commit that returned a generation before the
// reopen is readable with the same header, state, placements and receipt.
func WithReopen(reopen func(tb assert.TB, s store.Store) store.Store) Option {
	return func(c *config) { c.reopen = reopen }
}

// WithLostCommits gives the suite a way to make the next commit to s fail with an error
// that wraps version.ErrOutcomeUnknown, at the point that l selects. The suite then checks
// that Outcome and a retry settle both outcomes, and adds up to two lost commits to each
// case of the machine of commits.
func WithLostCommits(lose func(tb assert.TB, s store.Store, l Loss)) Option {
	return func(c *config) { c.lose = lose }
}

// WithDelete gives the suite a way to delete the scope sc from the storage behind s, as an
// operator does before a namespace is created again under the same name. The suite then
// creates the scope with another incarnation, brings it to the old generation, and checks
// that a read, a page token, a watch and the receipts of the old incarnation return
// shard.ErrIncarnation or nothing.
func WithDelete(del func(tb assert.TB, s store.Store, sc shard.Scope)) Option {
	return func(c *config) { c.del = del }
}

// WithCompact gives the suite a way to discard the generations of every scope of s below
// their current generations, as the compaction of a backend does. The suite then checks
// that a page token and a watch at a discarded generation return store.ErrCompacted, and
// that a watch from the current generation continues.
func WithCompact(compact func(tb assert.TB, s store.Store)) Option {
	return func(c *config) { c.compact = compact }
}

// cleanupTB is an assert.TB that runs a function when its test or case ends, as a
// *testing.T and a *prop.Case do.
type cleanupTB interface {
	assert.TB
	// Cleanup registers f to run when the test or the case ends.
	Cleanup(f func())
}

// fixture contains the identities of the records of one run of the suite, and the
// generators of its mutations.
type fixture struct {
	// puts generates the placements of one commit.
	puts prop.Generator[[]shard.Placement]
	// mutations generates the mutations that one case of a law commits, in order.
	mutations prop.Generator[[]store.Mutation]
	// ranges lists the range of each shard of shards.
	ranges [shardCount]shard.Range
	// everywhere lists a placement of each shard with a replica on each node.
	everywhere []shard.Placement
	// mapping is the mapping of the namespace, which the first commit of the law of the
	// state records.
	mapping shard.Mapping
	// cell is the controller record that the last commit of the law of node watches
	// replaces, so every watcher receives its generation.
	cell shard.Cell
	// drains lists the keys of the generated drains: the manual drains of two nodes, and
	// the drains of two disruptions.
	drains [drainCount]store.DrainKey
	// shards lists the shards of the generated placements and transitions.
	shards [shardCount]shard.ID
	// digests lists the digest of each disruption of disruptions.
	digests [disruptionCount]crypto.Digest
	// proposals lists the IDs of the generated proposals.
	proposals [proposalCount]proposal.ID
	// disruptions lists the IDs of the generated disruptions.
	disruptions [disruptionCount]budget.ID
}

// newFixture returns the fixture of one run of the suite. The shards cover the positions
// in order, the first from the empty start and the last to the end of the position space.
func newFixture() *fixture {
	f := &fixture{
		mapping: shard.Mapping{Partitioner: partitioner, Config: []byte(`{"slots":16}`)},
		cell:    shard.Cell{ID: cell, Controller: controller, ControllerIdentity: caller, Epoch: lawTerm},
	}
	for i := range shardCount {
		f.shards[i] = shard.ID{ID: id.New128([16]byte{0: tagShard, 15: byte(i + 1)})}
		if i > 0 {
			f.ranges[i].Start = []byte{byte(i * rangeStep)}
		}
		if i < shardCount-1 {
			f.ranges[i].End = []byte{byte((i + 1) * rangeStep)}
		}
		f.everywhere = append(f.everywhere, shard.Placement{
			Shard: f.shards[i],
			Range: f.ranges[i],
			Replicas: []shard.Replica{
				{Node: nodeA, Role: shard.RolePrimary},
				{Node: nodeB, Role: shard.RoleSecondary},
				{Node: nodeC, Role: shard.RoleSecondary},
			},
		})
	}
	for i := range proposalCount {
		f.proposals[i] = proposal.ID{ID: id.New128([16]byte{0: tagProposal, 15: byte(i + 1)})}
	}
	for i := range disruptionCount {
		f.disruptions[i] = budget.ID{ID: id.New128([16]byte{0: tagDisruption, 15: byte(i + 1)})}
		f.digests[i] = sha256.Hasher{}.Hash(f.disruptions[i].Bytes())
	}
	f.drains = [...]store.DrainKey{
		{Node: nodeA},
		{Node: nodeB},
		{Node: nodeA, Disruption: f.disruptions[0]},
		{Node: nodeC, Disruption: f.disruptions[1]},
	}
	f.puts = prop.Composite(f.generatePuts)
	f.mutations = prop.List(prop.Composite(f.generateMutation), prop.MinSize(1), prop.MaxSize(maxCommits))
	return f
}

// generatePuts generates the placements of one commit. It places up to maxBatch shards,
// each with up to three replicas and an epoch.
func (f *fixture) generatePuts(c *prop.Case) []shard.Placement {
	idx := c.Draw(shardSubsets, "placed shards")
	ps := make([]shard.Placement, len(idx))
	for i, s := range idx {
		ps[i] = shard.Placement{
			Shard:    f.shards[s],
			Range:    f.ranges[s],
			Replicas: c.Draw(replicaSets, "replicas"),
			Epoch:    c.Draw(prop.Integer[epoch.Epoch](0, 3), "epoch"),
		}
	}
	return ps
}

// generateMutation generates one mutation of the laws over generated commits. The mutation
// has placements, transitions, ended shards, applied and forgotten proposals, reserved and
// released disruptions, drains and undrains, a spec and a controller record. Each list
// contains a shard, a proposal, a disruption or a drain at most once.
func (f *fixture) generateMutation(c *prop.Case) store.Mutation {
	m := store.Mutation{Put: f.generatePuts(c)}
	for _, s := range c.Draw(shardSubsets, "transitions") {
		m.Transitions = append(m.Transitions, store.Transition{
			Started:  c.Draw(instants, "started"),
			Protocol: protocol,
			Fallback: c.Draw(endpoints, "fallback"),
			From:     c.Draw(endpoints, "from"),
			To:       c.Draw(endpoints, "to"),
			Operation: shard.OperationID{
				Incarnation: incA,
				Shard:       f.shards[s],
				Sequence:    c.Draw(prop.Integer[uint64](1, 3), "sequence"),
			},
			NewEpoch:        c.Draw(prop.Integer[epoch.Epoch](1, 3), "new epoch"),
			Step:            c.Draw(prop.Integer(0, 3), "step"),
			ProtocolVersion: 1,
			Phase:           c.Draw(prop.SampledFrom(shard.PhaseRunning, shard.PhaseAborting), "phase"),
		})
	}
	for _, s := range c.Draw(shardSubsets, "ended shards") {
		m.Done = append(m.Done, f.shards[s])
	}
	for _, p := range c.Draw(proposalSubsets, "applied proposals") {
		m.Applied = append(m.Applied, store.Applied{Caller: caller, Proposal: proposal.Proposal{
			Scope:       scope,
			Expires:     c.Draw(instants, "expires"),
			Body:        []byte(`{}`),
			Base:        1,
			Incarnation: incA,
			ID:          f.proposals[p],
			Kind:        proposal.Move,
		}})
	}
	for _, p := range c.Draw(proposalSubsets, "forgotten proposals") {
		m.Forget = append(m.Forget, f.proposals[p])
	}
	for _, d := range c.Draw(disruptionSubsets, "reserved disruptions") {
		m.Reserve = append(m.Reserve, f.generateReservation(c, d))
	}
	for _, d := range c.Draw(disruptionSubsets, "released disruptions") {
		m.Release = append(m.Release, f.disruptions[d])
	}
	for _, k := range c.Draw(drainSubsets, "drains") {
		key := f.drains[k]
		m.Drain = append(m.Drain, store.Drain{
			Since:      c.Draw(instants, "since"),
			Node:       key.Node,
			Caller:     caller,
			Disruption: key.Disruption,
		})
	}
	for _, k := range c.Draw(drainSubsets, "undrains") {
		m.Undrain = append(m.Undrain, f.drains[k])
	}
	m.Spec = c.Draw(specs, "spec")
	if c.Draw(prop.Boolean(), "controller record") {
		record := f.cell
		record.Epoch = c.Draw(prop.Integer[epoch.Epoch](1, 3), "controller epoch")
		m.Cell = &record
	}
	return m
}

// generateReservation generates the reservation of the disruption d, a restart of one node
// for an hour. The reservation is pending or admitted.
func (f *fixture) generateReservation(c *prop.Case, d int) budget.Reservation {
	start := c.Draw(instants, "start")
	r := budget.Reservation{
		Owner: caller,
		Disruption: budget.Disruption{
			Start:       start,
			End:         start.Add(time.Hour),
			Scope:       scope,
			Nodes:       []shard.NodeID{c.Draw(nodes, "disrupted node")},
			ID:          f.disruptions[d],
			Incarnation: incA,
			Impact:      budget.ProcessRestart,
			Negotiable:  true,
		},
		Digest: f.digests[d],
	}
	if c.Draw(prop.Boolean(), "admitted") {
		r.Admitted = start
	}
	return r
}

// writerID is a writer of the machine of commits. It has a namespace incarnation and a
// term.
type writerID struct {
	inc  shard.NamespaceIncarnation
	term epoch.Epoch
}

// writers lists the writers of the machine of commits. The incarnation A writes at the
// terms 1, 2, 3 and zero, and the incarnation B at the term 2.
var writers = [...]writerID{{incA, 1}, {incA, 2}, {incA, 3}, {incA, 0}, {incB, 2}}

// receipt is one receipt in the model of a scope: the generation that its commit created,
// and the variant of the mutation of the commit. The zero receipt is no receipt.
type receipt struct {
	generation epoch.Epoch
	variant    int
}

// scopeModel is the state of one scope in the machine of commits: the header and the
// receipts that Head, Read and Outcome return.
//
// # Concurrency
//
// The machine never modifies a scopeModel that the spec of the machine has returned.
type scopeModel struct {
	// receipts lists the receipt of each commit ID of the machine, by writer and then by
	// number.
	receipts [len(writers) * seqs]receipt
	// generation is the generation of the scope.
	generation epoch.Epoch
	// watermark is the watermark of the scope.
	watermark epoch.Epoch
	// inc is the namespace incarnation that created the scope, or zero.
	inc shard.NamespaceIncarnation
}

// head returns the header of the scope that m models. A commit raises the schema of a
// scope to the schema of this release.
func (m *scopeModel) head() store.Head {
	h := store.Head{Generation: m.generation, Watermark: m.watermark, Incarnation: m.inc}
	if m.generation > 0 {
		h.Schema = store.Schema
	}
	return h
}

// call is a call of the machine of commits with its inputs, as the history records it.
type call interface {
	// next returns the state that the call leaves in the scope that m models, when the
	// call returned what op states, and nil when the scope cannot return that.
	next(m *scopeModel, op history.Operation) []*scopeModel
}

// headCall is a call of Head or of Read, which returns the header of the scope.
type headCall struct{}

// next returns m when the call returned the header of m.
func (headCall) next(m *scopeModel, op history.Operation) []*scopeModel {
	if !op.Returned(m.head()) {
		return nil
	}
	return []*scopeModel{m}
}

// commitCall is a call of Commit in the machine: the mutation of the writer w with the
// number seq and the variant, at the generation expect.
type commitCall struct {
	expect  epoch.Epoch
	w       int
	seq     int
	variant int
}

// next returns the state that the commit leaves in m, when the commit returned what it
// returns in m: the same generation, and an error that wraps the same definite rejection.
func (c commitCall) next(m *scopeModel, op history.Operation) []*scopeModel {
	want, n := c.apply(m)
	got, _ := op.Output.(commitResult)
	if op.Known && (got.generation != want.generation || !errors.Is(got.err, want.err)) {
		return nil
	}
	return []*scopeModel{n}
}

// apply returns what the commit returns in the scope that m models, and the state that it
// leaves. It checks the incarnation, the fence, the receipt and the generation, in that
// order, and does not modify m.
func (c commitCall) apply(m *scopeModel) (commitResult, *scopeModel) {
	w := writers[c.w]
	slot := c.w*seqs + c.seq - 1
	r := m.receipts[slot]
	if !m.inc.IsZero() && m.inc != w.inc {
		return commitResult{err: shard.ErrIncarnation}, m
	}
	if !epoch.Admissible(m.watermark, w.term) {
		return commitResult{err: epoch.ErrFenced}, m
	}
	if r.generation > 0 && r.variant == c.variant {
		return commitResult{generation: r.generation}, m
	}
	if r.generation > 0 {
		return commitResult{err: store.ErrReused}, m
	}
	if m.generation != c.expect {
		return commitResult{err: version.ErrMismatch}, m
	}
	n := *m
	if n.generation == 0 {
		n.inc = w.inc
	}
	n.generation++
	if w.term > n.watermark {
		n.watermark = w.term
		for i := range n.receipts {
			if writers[i/seqs].term < w.term {
				n.receipts[i] = receipt{}
			}
		}
	}
	if c.variant == confirming && c.seq > 1 {
		n.receipts[slot-1] = receipt{}
	}
	n.receipts[slot] = receipt{generation: n.generation, variant: c.variant}
	return commitResult{generation: n.generation}, &n
}

// outcomeCall is a call of Outcome in the machine for the commit ID of the writer w with
// the number seq.
type outcomeCall struct {
	w   int
	seq int
}

// next returns m when the call returned the receipt that m keeps for the commit ID.
func (c outcomeCall) next(m *scopeModel, op history.Operation) []*scopeModel {
	want := outcomeResult{}
	if r := m.receipts[c.w*seqs+c.seq-1]; r.generation > 0 {
		want = outcomeResult{generation: r.generation, variant: r.variant, ok: true}
	}
	if !op.Returned(want) {
		return nil
	}
	return []*scopeModel{m}
}

// commitSpec is the sequential specification of the machine of commits. Each call of its
// history records its call value as its only argument.
var commitSpec = history.Spec[*scopeModel]{
	Initial: func() *scopeModel { return &scopeModel{} },
	Next: func(m *scopeModel, op history.Operation) []*scopeModel {
		c, _ := op.Args[0].(call)
		return c.next(m, op)
	},
	Equal: func(a, b *scopeModel) bool { return *a == *b },
	Hash:  func(m *scopeModel) uint64 { return maphash.Comparable(modelSeed, *m) },
}

// commitResult is what a call of Commit returns in the machine: the generation that the
// commit created, or the error of the call.
type commitResult struct {
	// err is the error of the call. In the model, it is the definite rejection that the
	// error of the call wraps.
	err error
	// generation is the generation that the commit created.
	generation epoch.Epoch
}

// outcomeResult is what a call of Outcome returns in the machine. ok reports whether the
// scope keeps the receipt, generation is the generation in the receipt, and variant is the
// variant of the mutation whose digest the receipt contains, or -1 for another digest.
type outcomeResult struct {
	generation epoch.Epoch
	variant    int
	ok         bool
}

// table lists the commit IDs, the mutations and the digests of the machine of commits, by
// writer, number and variant.
type table struct {
	mutations [len(writers)][seqs][variants]store.Mutation
	digests   [len(writers)][seqs][variants]crypto.Digest
	ids       [len(writers)][seqs]store.CommitID
}

// newTable returns the table of the machine of commits. The mutations of one commit ID
// differ in their specs, and the mutation of the variant confirming confirms the commit of
// its writer before it. It fails tb when a mutation does not encode.
func newTable(tb assert.TB) *table {
	tb.Helper()
	tab := &table{}
	for w, wr := range writers {
		for s := range seqs {
			tab.ids[w][s] = store.CommitID{Incarnation: wr.inc, Term: wr.term, Seq: uint64(s + 1)}
			for v := range variants {
				m := &tab.mutations[w][s][v]
				m.ID = tab.ids[w][s]
				m.Spec = []byte(strconv.Itoa(v))
				if v == confirming && s > 0 {
					m.Confirm = []store.CommitID{tab.ids[w][s-1]}
				}
				d, err := m.Digest()
				assert.NoError(tb, err, "each mutation of the machine of commits encodes")
				tab.digests[w][s][v] = d
			}
		}
	}
	return tab
}

// machine is the subject of the machine of commits in one case. It has a store, the scope
// of its calls, the table of their mutations, and the count of the commits that it lost.
type machine struct {
	// s is the store of the case.
	s store.Store
	// tab is the table of the mutations of the commits.
	tab *table
	// lose makes the next commit fail, or is nil without WithLostCommits.
	lose func(tb assert.TB, s store.Store, l Loss)
	// sc is the scope of the calls.
	sc shard.Scope
	// lost counts the calls of loseNext in the case.
	lost atomic.Int32
}

// actions returns the actions of the machine. They call Head, Read and Outcome, Commit at
// an arbitrary generation, and Commit at the generation that Head returns. WithLostCommits
// adds an action that loses the next commit.
func (m *machine) actions() []stateful.Action[*scopeModel] {
	actions := []stateful.Action[*scopeModel]{
		{Name: opHead, Run: func(c *prop.Case, client int, _ any) { m.head(c, client) }},
		{Name: opRead, Run: m.read},
		{Name: opOutcome, Input: generateOutcome, Run: m.outcome},
		{Name: opCommit, Input: generateCommit, Run: m.commitAt},
		{Name: opCommitAtHead, Input: generateCommit, Run: m.commitAtHead},
	}
	if m.lose != nil {
		actions = append(actions, stateful.Action[*scopeModel]{Name: opLose, Input: generateLoss, Run: m.loseNext})
	}
	return actions
}

// head calls Head on client, and returns the header. It records the call in the history
// of c.
func (m *machine) head(c *prop.Case, client int) store.Head {
	call := c.History().Invoke(client, opHead, []any{headCall{}})
	h, err := m.s.Head(c.Context(), m.sc)
	assert.NoError(c, err, "Head returns the header of the scope")
	call.OK(h)
	return h
}

// read calls Read on client, and records the call with the header of the state that Read
// returns.
func (m *machine) read(c *prop.Case, client int, _ any) {
	call := c.History().Invoke(client, opRead, []any{headCall{}})
	state, cur, err := m.s.Read(c.Context(), m.sc, shard.NamespaceIncarnation{}, store.Query{}, page.Page{Limit: 1})
	assert.NoError(c, err, "Read returns the state of the scope")
	assert.NoError(c, cur.Close(), "Close of a cursor returns nil")
	call.OK(state.Head)
}

// outcome calls Outcome on client for the commit ID of the input, and records the call
// with the generation and the variant of the receipt that Outcome returns.
func (m *machine) outcome(c *prop.Case, client int, in any) {
	oc, _ := in.(outcomeCall)
	call := c.History().Invoke(client, opOutcome, []any{oc})
	cid := m.tab.ids[oc.w][oc.seq-1]
	r, ok, err := m.s.Outcome(c.Context(), m.sc, cid)
	assert.NoError(c, err, "Outcome returns the receipt of a commit")
	got := outcomeResult{}
	if ok {
		assert.Equal(c, r.ID, cid, "Outcome returns the receipt of the commit ID that it reads")
		got = outcomeResult{
			generation: r.Generation,
			variant:    slices.Index(m.tab.digests[oc.w][oc.seq-1][:], r.Digest),
			ok:         true,
		}
	}
	call.OK(got)
}

// commitAt calls Commit on client at the generation of the input.
func (m *machine) commitAt(c *prop.Case, client int, in any) {
	cc, _ := in.(commitCall)
	m.commit(c, client, cc)
}

// commitAtHead calls Head on client, and then Commit at the generation that Head returned.
func (m *machine) commitAtHead(c *prop.Case, client int, in any) {
	cc, _ := in.(commitCall)
	cc.expect = m.head(c, client).Generation
	m.commit(c, client, cc)
}

// commit calls Commit for cc on client, and records the call in the history of c with the
// generation or the definite rejection that it returned. A commit whose outcome is unknown
// completes as unknown. The case counts the label of the outcome.
func (m *machine) commit(c *prop.Case, client int, cc commitCall) {
	call := c.History().Invoke(client, opCommit, []any{cc})
	w := writers[cc.w]
	mu := &m.tab.mutations[cc.w][cc.seq-1][cc.variant]
	g, err := m.s.Writer(m.sc, w.inc, w.term).Commit(c.Context(), cc.expect, mu)
	if errors.Is(err, version.ErrOutcomeUnknown) {
		c.Classify(labelUnknown)
		call.Unknown(err)
		return
	}
	if err != nil {
		for _, r := range rejections {
			if errors.Is(err, r.err) {
				c.Classify(r.label)
			}
		}
		call.OK(commitResult{err: err})
		return
	}
	if g == cc.expect+1 {
		c.Classify(labelApplied)
	} else {
		c.Classify(labelReplayed)
	}
	call.OK(commitResult{generation: g})
}

// loseNext makes the next commit to the store fail at the point that the input selects.
// It arms no fault after the first maxLost calls of a case.
func (m *machine) loseNext(c *prop.Case, _ int, in any) {
	if m.lost.Add(1) > maxLost {
		return
	}
	l, _ := in.(Loss)
	m.lose(c, m.s, l)
}

// lease is the state of the election of a scope in the law of terms: the last term that
// the election granted, and whether a candidate leads.
type lease struct {
	last epoch.Epoch
	held bool
}

// termSpec is the sequential specification of the election of a scope. A candidate leads
// only while no other candidate leads, and leads with a term above every term before it
// until it resigns. Next builds the state that a call leaves, and returns it when the call
// is valid in l.
var termSpec = history.Spec[lease]{
	Initial: func() lease { return lease{} },
	Next: func(l lease, op history.Operation) []lease {
		term, _ := op.Output.(epoch.Epoch)
		next, valid := lease{last: l.last}, l.held
		if op.Name == opLead {
			next, valid = lease{last: max(term, l.last), held: true}, !l.held && (!op.Known || term > l.last)
		}
		return slices.DeleteFunc([]lease{next}, func(lease) bool { return !valid })
	},
}

// fenced is a handle of core's fence laws: a writer of one scope of a store at one term,
// with the counter of the commit numbers of the scope.
type fenced struct {
	// s is the store of the scope.
	s store.Store
	// seq counts the commits of the scope.
	seq *atomic.Uint64
	// sc is the scope.
	sc shard.Scope
	// term is the term of the writer.
	term epoch.Epoch
}

// committer commits the mutations of one law to the scope of a store, in order, each at the
// generation before it, through a writer of one incarnation at the term lawTerm.
type committer struct {
	// w is the writer of the commits.
	w store.Writer
	// inc is the namespace incarnation of the writer.
	inc shard.NamespaceIncarnation
	// seq is the number of the last commit.
	seq uint64
	// gen is the generation of the scope after the last commit.
	gen epoch.Epoch
}

// commit sets the ID of m to the next commit ID of the committer, commits m, and returns
// the generation that m created. It fails tb when the commit does not apply at the
// generation after the last one.
func (cm *committer) commit(tb assert.TB, m *store.Mutation) epoch.Epoch {
	tb.Helper()
	cm.seq++
	m.ID = store.CommitID{Incarnation: cm.inc, Term: lawTerm, Seq: cm.seq}
	g, err := cm.w.Commit(assert.Context(tb), cm.gen, m)
	assert.NoError(tb, err, "Commit applies a mutation at the generation of the scope")
	assert.Equal(tb, g, cm.gen+1, "Commit returns the generation after the one that it expects")
	cm.gen = g
	return g
}

// stateModel is the state of a scope apart from its header and its placements, as the
// mutations of a law leave it.
type stateModel struct {
	applied      map[proposal.ID]time.Time
	transitions  map[shard.ID]store.Transition
	reservations map[budget.ID]budget.Reservation
	drains       map[store.DrainKey]store.Drain
	spec         []byte
	mapping      shard.Mapping
	cell         shard.Cell
}

// newStateModel returns the model of a scope before its first commit.
func newStateModel() *stateModel {
	return &stateModel{
		applied:      make(map[proposal.ID]time.Time),
		transitions:  make(map[shard.ID]store.Transition),
		reservations: make(map[budget.ID]budget.Reservation),
		drains:       make(map[store.DrainKey]store.Drain),
	}
}

// apply applies m in the order that store.Mutation states: Applied before Forget, Done
// before Transitions, Reserve before Release, and Drain before Undrain.
func (sm *stateModel) apply(m *store.Mutation) {
	for i := range m.Applied {
		sm.applied[m.Applied[i].Proposal.ID] = m.Applied[i].Proposal.Expires
	}
	for _, p := range m.Forget {
		delete(sm.applied, p)
	}
	for _, s := range m.Done {
		delete(sm.transitions, s)
	}
	for i := range m.Transitions {
		sm.transitions[m.Transitions[i].Operation.Shard] = m.Transitions[i]
	}
	for i := range m.Reserve {
		sm.reservations[m.Reserve[i].Disruption.ID] = m.Reserve[i]
	}
	for _, d := range m.Release {
		delete(sm.reservations, d)
	}
	for i := range m.Drain {
		sm.drains[store.DrainKey{Node: m.Drain[i].Node, Disruption: m.Drain[i].Disruption}] = m.Drain[i]
	}
	for _, k := range m.Undrain {
		delete(sm.drains, k)
	}
	if m.Spec != nil {
		sm.spec = m.Spec
	}
	if m.Mapping != nil {
		sm.mapping = *m.Mapping
	}
	if m.Cell != nil {
		sm.cell = *m.Cell
	}
}

// state returns the state that sm models with the header h, with its lists in the orders
// that store.State states.
func (sm *stateModel) state(h store.Head) store.State {
	return store.State{
		Applied: sm.applied,
		Spec:    sm.spec,
		Transitions: slices.SortedFunc(maps.Values(sm.transitions), func(a, b store.Transition) int {
			return a.Operation.Shard.Compare(b.Operation.Shard.ID)
		}),
		Reservations: slices.SortedFunc(maps.Values(sm.reservations), func(a, b budget.Reservation) int {
			return a.Disruption.ID.Compare(b.Disruption.ID.ID)
		}),
		Drains: slices.SortedFunc(maps.Values(sm.drains), func(a, b store.Drain) int {
			return cmp.Or(strings.Compare(string(a.Node), string(b.Node)), a.Disruption.Compare(b.Disruption.ID))
		}),
		Mapping: sm.mapping,
		Cell:    sm.cell,
		Head:    h,
	}
}

// watchModel is the placements and the transitions of a scope, as the mutations of a law
// leave them. It derives the events of a watch of one node from them.
type watchModel struct {
	placements  map[shard.ID]shard.Placement
	transitions map[shard.ID]store.Transition
}

// newWatchModel returns the model of a scope before its first commit.
func newWatchModel() *watchModel {
	return &watchModel{
		placements:  make(map[shard.ID]shard.Placement),
		transitions: make(map[shard.ID]store.Transition),
	}
}

// event applies m, the mutation of the generation g, and returns the event of g for a watch
// of node. It reports whether a change of m concerns node. Such a change puts a placement
// that names node or named it before, starts, advances or ends a transition with an
// endpoint on node, or replaces the controller record.
func (wm *watchModel) event(m *store.Mutation, g epoch.Epoch, node shard.NodeID) (store.Event, bool) {
	onNode := func(r shard.Replica) bool { return r.Node == node }
	involves := func(t *store.Transition) bool {
		return t.From.Node == node || t.To.Node == node || t.Fallback.Node == node
	}
	e := store.Event{Generation: g, Mutation: store.Mutation{ID: m.ID, Cell: m.Cell}}
	for i := range m.Put {
		p := &m.Put[i]
		before := wm.placements[p.Shard]
		now, was := slices.ContainsFunc(p.Replicas, onNode), slices.ContainsFunc(before.Replicas, onNode)
		if now || was {
			e.Mutation.Put = append(e.Mutation.Put, *p)
		}
		if was && !now {
			e.Removed = append(e.Removed, p.Shard)
		}
		wm.placements[p.Shard] = *p
	}
	for _, s := range m.Done {
		if t, ok := wm.transitions[s]; ok {
			if involves(&t) {
				e.Mutation.Done = append(e.Mutation.Done, s)
			}
			delete(wm.transitions, s)
		}
	}
	for i := range m.Transitions {
		t := &m.Transitions[i]
		if involves(t) {
			e.Mutation.Transitions = append(e.Mutation.Transitions, *t)
		}
		wm.transitions[t.Operation.Shard] = *t
	}
	changed := m.Cell != nil || len(e.Mutation.Put) > 0 || len(e.Mutation.Transitions) > 0 ||
		len(e.Mutation.Done) > 0
	return e, changed
}

// image is what a store returns for the scope. It has the header, the state and the
// placements of the scope, and the receipts of the commits of a committer.
type image struct {
	placements []shard.Placement
	receipts   []store.Receipt
	state      store.State
	head       store.Head
}

// suite is one run of AssertStore: the stores that newStore returns, the options, the
// fixture of the laws, and the table of the machine of commits.
type suite struct {
	// newStore returns an empty store.
	newStore func(tb assert.TB) store.Store
	// fx is the fixture of the laws.
	fx *fixture
	// tab is the table of the machine of commits.
	tab *table
	// cfg is the result of the options.
	cfg config
}

// open returns a store that newStore returns, and closes it when tb ends.
func (su *suite) open(tb cleanupTB) store.Store {
	tb.Helper()
	s := su.newStore(tb)
	tb.Cleanup(func() { assert.NoError(tb, s.Close(), "Close returns nil") })
	return s
}

// headLaws runs the laws of Head.
func (su *suite) headLaws(t *testing.T) {
	t.Parallel()
	t.Run("returns the zero Head of a scope before its first commit", func(t *testing.T) {
		t.Parallel()
		h, err := su.open(t).Head(t.Context(), scope)
		assert.NoError(t, err, "Head returns the header of a scope")
		assert.Equal(t, h, store.Head{}, "the Head of a scope before its first commit is zero")
	})
}

// readLaws runs the laws of Read.
func (su *suite) readLaws(t *testing.T) {
	t.Parallel()
	t.Run("returns the zero State of a scope before its first commit", su.readsNothing)
	t.Run("returns the state that the commits leave", su.readsState)
	t.Run("returns each selected placement once at the generation of the read", su.readsPages)
	t.Run("returns ErrIncarnation for a scope of another incarnation", su.readsOtherIncarnation)
	if su.cfg.del != nil {
		t.Run("returns ErrIncarnation for a scope that another incarnation created again", su.readsRecreated)
		t.Run("returns ErrIncarnation for a page token of the incarnation before", su.readsRecreatedToken)
	}
	if su.cfg.compact != nil {
		t.Run("returns ErrCompacted for a page token of a discarded generation", su.readsCompacted)
	}
	if su.cfg.reopen != nil {
		t.Run("returns the commits that applied before a reopen", su.readsReopened)
	}
}

// readsNothing checks that Read returns the zero State and an empty page for a scope before
// its first commit, whatever incarnation the reader expects.
func (su *suite) readsNothing(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "Read returns the zero State and an empty page for a scope before its first commit",
		func(c *prop.Case) {
			s := su.open(c)
			inc := c.Draw(prop.SampledFrom(shard.NamespaceIncarnation{}, incA, incB), "incarnation")
			state, cur, err := s.Read(c.Context(), scope, inc, store.Query{}, page.Page{Limit: maxLimit})
			assert.NoError(c, err, "Read returns the state of a scope before its first commit")
			placements, token := drain(c, cur, maxLimit)
			assert.Equal(c, state, store.State{}, "the State of a scope before its first commit is zero",
				assert.EquateEmpty())
			assert.Empty(c, placements, "a scope before its first commit does not have a placement")
			assert.Equal(c, token, "", "the first page of a scope before its first commit is its last")
		})
}

// readsState checks that Read returns the state that generated commits leave.
func (su *suite) readsState(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "Read returns the state that the commits leave", func(c *prop.Case) {
		s := su.open(c)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		ms := c.Draw(su.fx.mutations, "mutations")
		ms[0].Mapping = &su.fx.mapping
		sm := newStateModel()
		for i := range ms {
			cm.commit(c, &ms[i])
			sm.apply(&ms[i])
		}
		got, cur, err := s.Read(c.Context(), scope, incA, store.Query{}, page.Page{})
		assert.NoError(c, err, "Read returns the state of the scope")
		assert.NoError(c, cur.Close(), "Close of a cursor returns nil")
		want := sm.state(store.Head{Generation: cm.gen, Watermark: lawTerm, Schema: store.Schema, Incarnation: incA})
		assert.Equal(c, got, want, "Read returns the state that the commits leave", assert.EquateEmpty())
	})
}

// readsPages checks that the pages of a Read list each selected placement once, in position
// order, at the generation of the first page, also when a commit applies between two
// pages.
func (su *suite) readsPages(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "the pages of a Read list each selected placement once, in position order, "+
		"at the generation of the read", func(c *prop.Case) {
		s := su.open(c)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		batches := c.Draw(prop.List(su.fx.puts, prop.MinSize(1), prop.MaxSize(maxCommits)), "batches")
		for _, b := range batches {
			cm.commit(c, &store.Mutation{Put: b})
		}
		limit := c.Draw(prop.Integer(1, maxLimit), "limit")
		node := c.Draw(prop.SampledFrom[shard.NodeID]("", nodeA, nodeB, nodeC), "node")
		between := c.Draw(prop.Boolean(), "commit between the pages")
		q := store.Query{Node: node}
		first, cur, err := s.Read(c.Context(), scope, incA, q, page.Page{Limit: limit})
		assert.NoError(c, err, "Read returns the first page of the scope")
		got, token := drain(c, cur, limit)
		if between {
			cm.commit(c, &store.Mutation{Put: slices.Clone(su.fx.everywhere)})
		}
		for later := 1; token != ""; later++ {
			assert.True(c, later <= shardCount, "Read ends its pages within one page per placement of the scope")
			state, next, err := s.Read(c.Context(), scope, incA, q, page.Page{Token: token, Limit: limit})
			c.Assume(!errors.Is(err, store.ErrCompacted))
			assert.NoError(c, err, "Read returns the page of its token")
			assert.Equal(c, state.Head.Generation, first.Head.Generation,
				"every page of a read is at the generation of its first page")
			var placements []shard.Placement
			placements, token = drain(c, next, limit)
			got = append(got, placements...)
		}
		assert.Equal(c, got, selected(batches, node), "the pages list each selected placement once, in position order",
			assert.EquateEmpty())
	})
}

// readsOtherIncarnation checks that Read returns shard.ErrIncarnation for a scope that
// another incarnation created.
func (su *suite) readsOtherIncarnation(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	_, _, err := s.Read(t.Context(), scope, incB, store.Query{}, page.Page{})
	assert.ErrorIs(t, err, shard.ErrIncarnation, "Read returns ErrIncarnation for a scope of another incarnation")
}

// readsRecreated checks that Read returns shard.ErrIncarnation to a reader of an
// incarnation whose scope another incarnation created again.
func (su *suite) readsRecreated(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	su.recreate(t, s)
	_, _, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{})
	assert.ErrorIs(t, err, shard.ErrIncarnation,
		"Read returns ErrIncarnation for a scope that another incarnation created again")
}

// readsRecreatedToken checks that Read returns shard.ErrIncarnation for a page token of an
// incarnation whose scope another incarnation created again at the same generation.
func (su *suite) readsRecreatedToken(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	token := su.recreate(t, s)
	_, _, err := s.Read(
		t.Context(),
		scope,
		shard.NamespaceIncarnation{},
		store.Query{},
		page.Page{Token: token, Limit: 1},
	)
	assert.ErrorIs(
		t,
		err,
		shard.ErrIncarnation,
		"Read returns ErrIncarnation for a page token of the incarnation before",
	)
}

// readsCompacted checks that Read returns store.ErrCompacted for a page token of a
// generation that the store discarded.
func (su *suite) readsCompacted(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{Put: slices.Clone(su.fx.everywhere[:2])})
	_, cur, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Limit: 1})
	assert.NoError(t, err, "Read returns the first page of the scope")
	_, token := drain(t, cur, 1)
	cm.commit(t, &store.Mutation{Spec: []byte(`{}`)})
	su.cfg.compact(t, s)
	_, _, err = s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Token: token, Limit: 1})
	assert.ErrorIs(t, err, store.ErrCompacted, "Read returns ErrCompacted for a page token of a discarded generation")
}

// readsReopened checks that a store over the same storage returns the header, the state,
// the placements and the receipts of the commits that applied before a reopen.
func (su *suite) readsReopened(t *testing.T) {
	t.Parallel()
	s := su.newStore(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{Put: slices.Clone(su.fx.everywhere[:3])})
	cm.commit(t, &store.Mutation{Put: slices.Clone(su.fx.everywhere[3:]), Spec: []byte(`{}`)})
	before := capture(t, s, cm)
	r := su.cfg.reopen(t, s)
	t.Cleanup(func() { assert.NoError(t, r.Close(), "Close returns nil") })
	assert.Equal(t, capture(t, r, cm), before,
		"a store that opens again returns the commits that applied before", assert.EquateEmpty())
}

// recreate commits two generations to the scope of s through a writer of the incarnation
// A, and reads the first page of the scope with a limit of one placement. It then deletes
// the scope, commits two generations through a writer of the incarnation B, and returns
// the token of the page.
func (su *suite) recreate(t *testing.T, s store.Store) string {
	t.Helper()
	a := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	a.commit(t, &store.Mutation{Put: slices.Clone(su.fx.everywhere[:2])})
	a.commit(t, &store.Mutation{Spec: []byte(`{}`)})
	_, cur, err := s.Read(t.Context(), scope, incA, store.Query{}, page.Page{Limit: 1})
	assert.NoError(t, err, "Read returns the first page of the scope")
	_, token := drain(t, cur, 1)
	assert.NotEqual(t, token, "", "Read returns a token for a scope of two placements and a limit of one")
	su.cfg.del(t, s, scope)
	b := &committer{w: s.Writer(scope, incB, lawTerm), inc: incB}
	b.commit(t, &store.Mutation{Put: slices.Clone(su.fx.everywhere[:2])})
	b.commit(t, &store.Mutation{Spec: []byte(`{}`)})
	return token
}

// watchLaws runs the laws of Watch.
func (su *suite) watchLaws(t *testing.T) {
	t.Parallel()
	t.Run("yields each later generation once with its whole mutation", su.watchesWhole)
	t.Run("yields each change that concerns a node", su.watchesNode)
	t.Run("yields each commit that applies after the watch started", su.watchesLater)
	t.Run("ends once its context ends", su.watchesUntilCancel)
	t.Run("yields ErrIncarnation for a scope of another incarnation", su.watchesOtherIncarnation)
	t.Run("yields ErrIncarnation when another incarnation creates the scope", su.watchesCreation)
	if su.cfg.del != nil {
		t.Run("yields ErrIncarnation for a scope that another incarnation created again", su.watchesRecreated)
	}
	if su.cfg.compact != nil {
		t.Run("yields ErrCompacted from a discarded generation", su.watchesCompacted)
		t.Run("yields the generations after the current generation of a compacted scope", su.watchesAfterCompaction)
	}
}

// watchesWhole checks that a watch without a node yields each generation after its start
// once, in order, with the whole mutation of its commit.
func (su *suite) watchesWhole(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "Watch yields each later generation once, in order, with its whole mutation", func(c *prop.Case) {
		s := su.open(c)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		ms := c.Draw(su.fx.mutations, "mutations")
		for i := range ms {
			cm.commit(c, &ms[i])
		}
		from := c.Draw(prop.Integer(0, len(ms)-1), "from")
		inc := c.Draw(prop.SampledFrom(shard.NamespaceIncarnation{}, incA), "incarnation")
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		events := until(c, streamtest.Follow(ctx, s.Watch(ctx, scope, inc, epoch.Epoch(from), "")), epoch.Epoch(from),
			cm.gen)
		assert.Length(c, events, len(ms)-from, "Watch yields one event per generation")
		for i := range events {
			e := &events[i]
			assert.Equal(c, e.Generation, epoch.Epoch(from+i+1), "Watch yields the generations in order")
			assert.Equal(c, e.Mutation, ms[from+i], "an event contains the whole mutation of its commit",
				assert.EquateEmpty())
			assert.Empty(c, e.Removed, "an event of a watch without a node lists no removed shard")
		}
	})
}

// watchesNode checks that a watch of a node yields each change that concerns the node,
// with the shards whose placements stop naming the node, and progress events without a
// change.
func (su *suite) watchesNode(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "a Watch of a node yields each change that concerns the node", func(c *prop.Case) {
		s := su.open(c)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		ms := c.Draw(su.fx.mutations, "mutations")
		ms[len(ms)-1].Cell = &su.fx.cell
		node := c.Draw(nodes, "node")
		wm := newWatchModel()
		var want []store.Event
		for i := range ms {
			g := cm.commit(c, &ms[i])
			if e, ok := wm.event(&ms[i], g, node); ok {
				want = append(want, e)
			}
		}
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		events := until(c, streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, node)), 0, cm.gen)
		assert.Pairwise(
			c,
			events,
			func(earlier, later store.Event) bool { return earlier.Generation < later.Generation },
			"Watch yields the generations in order",
		)
		var changes []store.Event
		for i := range events {
			e := &events[i]
			if e.Mutation.ID != (store.CommitID{}) {
				changes = append(changes, *e)
				continue
			}
			assert.Equal(c, e.Mutation, store.Mutation{}, "a progress event contains no change", assert.EquateEmpty())
			assert.Empty(c, e.Removed, "a progress event lists no removed shard")
		}
		assert.Length(c, changes, len(want), "Watch yields one event for each generation with a change of the node")
		for i := range want {
			sameChange(c, &changes[i], &want[i])
		}
	})
}

// watchesLater checks that a watch yields each commit that applies after the watch
// started, as the commit applies.
func (su *suite) watchesLater(t *testing.T) {
	t.Parallel()
	prop.ForAll(t, "Watch yields each commit that applies after the watch started", func(c *prop.Case) {
		s := su.open(c)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		ms := c.Draw(su.fx.mutations, "mutations")
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
		for i := range ms {
			g := cm.commit(c, &ms[i])
			it, ok := streamtest.Receive(c, items, waitFor, "Watch yields a commit within the wait of the suite")
			assert.True(c, ok, "Watch yields every commit before its context ends")
			assert.NoError(c, it.Err, "Watch yields the generations of the scope")
			assert.Equal(c, it.Value.Generation, g, "Watch yields the generation of the commit")
			assert.Equal(c, it.Value.Mutation, ms[i], "an event contains the whole mutation of its commit",
				assert.EquateEmpty())
		}
	})
}

// watchesUntilCancel checks that a watch ends once its context ends.
func (su *suite) watchesUntilCancel(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
	_, ok := streamtest.Receive(
		t,
		items,
		waitFor,
		"Watch yields the generation of the scope within the wait of the suite",
	)
	assert.True(t, ok, "Watch yields the generation of the scope")
	cancel()
	streamtest.End(t, items, waitFor, "Watch ends once its context ends")
}

// watchesOtherIncarnation checks that a watch of a scope of another incarnation yields
// shard.ErrIncarnation and ends.
func (su *suite) watchesOtherIncarnation(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	endsWith(t, s, incB, 0, shard.ErrIncarnation, "Watch yields ErrIncarnation for a scope of another incarnation")
}

// watchesCreation checks that a watch of a scope before its first commit yields
// shard.ErrIncarnation and ends when another incarnation creates the scope.
func (su *suite) watchesCreation(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incB, 0, ""))
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	it, ok := streamtest.Receive(t, items, waitFor, "Watch yields an error within the wait of the suite")
	assert.True(t, ok, "Watch yields an error before it ends")
	assert.ErrorIs(
		t,
		it.Err,
		shard.ErrIncarnation,
		"Watch yields ErrIncarnation when another incarnation creates the scope",
	)
	streamtest.End(t, items, waitFor, "Watch ends after ErrIncarnation")
}

// watchesRecreated checks that a watch of an incarnation whose scope another incarnation
// created again yields shard.ErrIncarnation and ends.
func (su *suite) watchesRecreated(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	su.recreate(t, s)
	endsWith(t, s, incA, 1, shard.ErrIncarnation,
		"Watch yields ErrIncarnation for a scope that another incarnation created again")
}

// watchesCompacted checks that a watch from a generation that the store discarded yields
// store.ErrCompacted and ends.
func (su *suite) watchesCompacted(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	cm.commit(t, &store.Mutation{Spec: []byte(`{}`)})
	su.cfg.compact(t, s)
	endsWith(t, s, incA, 0, store.ErrCompacted, "Watch yields ErrCompacted from a discarded generation")
}

// watchesAfterCompaction checks that a watch from the current generation of a compacted
// scope yields the commits after it.
func (su *suite) watchesAfterCompaction(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(t, &store.Mutation{})
	cm.commit(t, &store.Mutation{Spec: []byte(`{}`)})
	su.cfg.compact(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, cm.gen, ""))
	m := store.Mutation{Spec: []byte(`{"version":1}`)}
	g := cm.commit(t, &m)
	it, ok := streamtest.Receive(t, items, waitFor, "Watch yields a commit within the wait of the suite")
	assert.True(t, ok, "Watch yields the commit after a compaction")
	assert.NoError(t, it.Err, "Watch yields the generations after the current generation of a compacted scope")
	assert.Equal(t, it.Value.Generation, g, "Watch yields the generation of the commit after a compaction")
}

// writerLaws runs the laws of Writer and its method Commit.
func (su *suite) writerLaws(t *testing.T) {
	t.Parallel()
	t.Run("Commit", su.commitLaws)
}

// commitLaws runs the laws of Commit, core's fence laws among them.
func (su *suite) commitLaws(t *testing.T) {
	t.Parallel()
	su.fences(t)
	t.Run("is linearizable with Head, Read and Outcome", su.commitsLinearizably)
	t.Run("returns the error of the encoding of a mutation that does not encode", su.commitsUnencodable)
	tests := []struct {
		name    string
		loss    Loss
		applied bool
	}{
		{name: "applies the retry of a lost request once", loss: LoseRequest},
		{name: "returns the generation of a lost reply to its retry", loss: LoseReply, applied: true},
	}
	if su.cfg.lose != nil {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				su.settles(t, tt.loss, tt.applied)
			})
		}
	}
}

// fences runs core's fence laws over Commit, through a store that the laws share.
func (su *suite) fences(t *testing.T) {
	t.Helper()
	s := su.open(t)
	ctx := t.Context()
	write := func(f fenced) error {
		h, err := f.s.Head(ctx, f.sc)
		expect.NoError(t, err, "Head returns the header of the scope")
		seq := f.seq.Add(1)
		m := store.Mutation{
			ID:   store.CommitID{Incarnation: incA, Term: f.term, Seq: seq},
			Spec: []byte(strconv.FormatUint(seq, 10)),
		}
		if _, err := f.s.Writer(f.sc, incA, f.term).Commit(ctx, h.Generation, &m); err != nil {
			return fmt.Errorf("storetest: commit to %v at the term %d: %w", f.sc, f.term, err)
		}
		return nil
	}
	var scopes atomic.Int64
	epochtest.AssertFencedWriter(t, epochtest.FencedSystem[fenced]{
		NewScope: func() epochtest.FencedScope[fenced] {
			sc := shard.Scope{
				Namespace: namespace,
				Cell:      shard.CellID(fencedCell + strconv.FormatInt(scopes.Add(1), 10)),
			}
			seq := &atomic.Uint64{}
			return epochtest.FencedScope[fenced]{
				Open: func(e epoch.Epoch) (fenced, error) {
					return fenced{s: s, seq: seq, sc: sc, term: e}, nil
				},
				Supersede: func(e epoch.Epoch) {
					expect.NoError(t, write(fenced{s: s, seq: seq, sc: sc, term: e}),
						"the commit of the term that supersedes the writer applies")
				},
				Snapshot: func() any {
					state, cur, err := s.Read(ctx, sc, incA, store.Query{}, page.Page{})
					expect.NoError(t, err, "Read returns the state of the scope")
					expect.NoError(t, cur.Close(), "Close of a cursor returns nil")
					return state
				},
			}
		},
		Writes: map[string]func(fenced) error{"applies the fence laws of core": write},
	})
}

// commitsLinearizably runs the machine of commits: Commit, Head, Read and Outcome on two
// clients, linearizable against the model of a scope. The run requires each outcome of
// Commit in a share of its cases, and the unknown outcome with WithLostCommits.
func (su *suite) commitsLinearizably(t *testing.T) {
	t.Parallel()
	opts := []prop.Option{
		prop.Require(labelApplied, shareHalf),
		prop.Require(labelIncarnation, shareHalf),
		prop.Require(labelMismatch, shareThird),
		prop.Require(labelFenced, shareThird),
		prop.Require(labelReused, shareQuarter),
		prop.Require(labelReplayed, shareQuarter),
	}
	if su.cfg.lose != nil {
		opts = append(opts, prop.Require(labelUnknown, shareThird))
	}
	prop.ForAll(t, "Commit, Head, Read and Outcome are linearizable against the model of a scope",
		func(c *prop.Case) {
			m := &machine{s: su.open(c), tab: su.tab, lose: su.cfg.lose, sc: scope}
			stateful.Steps(c, stateful.Machine[*scopeModel]{Spec: commitSpec, Actions: m.actions()},
				stateful.Clients(2))
		}, opts...)
}

// commitsUnencodable checks that Commit returns the error of the encoding of a mutation that
// does not encode, and leaves the scope unchanged.
func (su *suite) commitsUnencodable(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	m := store.Mutation{
		ID: store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1},
		Transitions: []store.Transition{{
			Operation: shard.OperationID{Incarnation: incA, Shard: su.fx.shards[0], Sequence: 1},
			Phase:     invalidPhase,
		}},
	}
	_, err := s.Writer(scope, incA, lawTerm).Commit(t.Context(), 0, &m)
	assert.ErrorIs(t, err, shard.ErrUnknownPhase, "Commit returns the error of the encoding of a mutation")
	h, err := s.Head(t.Context(), scope)
	assert.NoError(t, err, "Head returns the header of the scope")
	assert.Equal(t, h, store.Head{}, "a mutation that does not encode leaves the scope unchanged")
}

// settles checks that Outcome reports whether a commit that the fault l interrupted
// applied, and that a retry of the commit applies it at most once.
func (su *suite) settles(t *testing.T, l Loss, applied bool) {
	t.Helper()
	s := su.open(t)
	w := s.Writer(scope, incA, lawTerm)
	m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1}, Spec: []byte(`{}`)}
	d, err := m.Digest()
	assert.NoError(t, err, "the mutation of the law encodes")
	su.cfg.lose(t, s, l)
	_, err = w.Commit(t.Context(), 0, &m)
	assert.ErrorIs(
		t,
		err,
		version.ErrOutcomeUnknown,
		"Commit returns ErrOutcomeUnknown for a commit that a fault interrupts",
	)
	_, ok, err := s.Outcome(t.Context(), scope, m.ID)
	assert.NoError(t, err, "Outcome returns the receipt of a commit")
	assert.Equal(t, ok, applied, "Outcome reports the receipt of a lost commit exactly when the commit applied")
	g, err := w.Commit(t.Context(), 0, &m)
	assert.NoError(t, err, "the retry of a lost commit returns the generation of the commit")
	assert.Equal(t, g, epoch.Epoch(1), "the retry of a lost commit returns the generation that the commit created")
	r, ok, err := s.Outcome(t.Context(), scope, m.ID)
	assert.NoError(t, err, "Outcome returns the receipt of a commit")
	assert.True(t, ok, "Outcome reports the receipt of a settled commit")
	assert.Equal(t, r, store.Receipt{ID: m.ID, Digest: d, Generation: 1},
		"the receipt of a settled commit contains its digest and its generation")
}

// outcomeLaws runs the laws of Outcome apart from the machine of commits.
func (su *suite) outcomeLaws(t *testing.T) {
	t.Parallel()
	if su.cfg.del != nil {
		t.Run(
			"returns no receipt of an incarnation whose scope another incarnation created again",
			su.outcomesRecreated,
		)
	}
}

// outcomesRecreated checks that Outcome returns no receipt of an incarnation whose scope
// another incarnation created again.
func (su *suite) outcomesRecreated(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	su.recreate(t, s)
	_, ok, err := s.Outcome(t.Context(), scope, store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 2})
	assert.NoError(t, err, "Outcome returns the receipt of a commit")
	assert.False(t, ok, "a receipt does not outlive the incarnation of its commit")
}

// campaignLaws runs the laws of Campaign and of the terms that it grants.
func (su *suite) campaignLaws(t *testing.T) {
	t.Parallel()
	t.Run("grants each term to one candidate at a time", su.campaignsExclusively)
	t.Run("grants a term above the watermark of the scope", su.campaignsAboveWatermark)
	t.Run("returns the error of its context while another candidate leads", su.campaignsUntilCancel)
	t.Run("ends the term once the context of the campaign ends", su.campaignsWithContext)
	t.Run("grants the next term once the leader resigns", su.campaignsAfterResign)
}

// campaignsExclusively checks with concurrent candidates that the election of a scope
// grants each term to one candidate at a time, each term above the terms before it. Each
// candidate campaigns once with an ended context, and that campaign returns the error of
// its context.
func (su *suite) campaignsExclusively(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	live := t.Context()
	ended, cancel := context.WithCancel(live)
	cancel()
	h := history.New()
	outcomes := history.Concurrently(candidates, waitFor, func(client int) (any, error) {
		for round := range rounds {
			ctx := live
			if (client+round)%candidates == 0 {
				ctx = ended
			}
			call := h.Invoke(client, opLead, nil)
			l, err := s.Campaign(ctx, scope)
			if err != nil {
				expect.ErrorIs(t, err, context.Canceled, "Campaign returns an error only for an ended context")
				call.Fail(err)
				continue
			}
			call.OK(l.Term())
			call = h.Invoke(client, opResign, nil)
			expect.NoError(t, l.Resign(live), "Resign returns nil")
			call.OK(nil)
		}
		return client, nil
	})
	for _, o := range outcomes {
		assert.True(t, o.Finished, "each candidate leads its terms within the wait of the suite")
	}
	history.Linearizable(
		t,
		h,
		termSpec,
		"Campaign grants each term to one candidate at a time, above the terms before it",
	)
}

// campaignsAboveWatermark checks that the election of a scope grants a term above the
// watermark of the scope.
func (su *suite) campaignsAboveWatermark(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: highTerm, Seq: 1}}
	_, err := s.Writer(scope, incA, highTerm).Commit(t.Context(), 0, &m)
	assert.NoError(t, err, "Commit applies the commit that raises the watermark")
	l, err := s.Campaign(t.Context(), scope)
	assert.NoError(t, err, "Campaign grants a term")
	assert.True(t, l.Term() > highTerm, "Campaign grants a term above the watermark of the scope")
	assert.NoError(t, l.Resign(t.Context()), "Resign returns nil")
}

// campaignsUntilCancel checks that Campaign returns the error of its context while another
// candidate leads.
func (su *suite) campaignsUntilCancel(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	l, err := s.Campaign(t.Context(), scope)
	assert.NoError(t, err, "Campaign grants a term")
	assert.HonoursCancellation(t, func(ctx context.Context) error {
		_, err := s.Campaign(ctx, scope)
		// The wrap of a nil error matches no cancellation, so a Campaign that returns nil
		// fails the check.
		return fmt.Errorf("storetest: campaign while another candidate leads: %w", err)
	}, "Campaign returns the error of its context while another candidate leads")
	assert.NoError(t, l.Resign(t.Context()), "Resign returns nil")
}

// campaignsWithContext checks that a term ends once the context of the campaign that
// granted it ends.
func (su *suite) campaignsWithContext(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l, err := s.Campaign(ctx, scope)
	assert.NoError(t, err, "Campaign grants a term")
	cancel()
	streamtest.Receive(t, l.Done(), waitFor, "Done is closed once the context of the campaign ends")
}

// campaignsAfterResign checks that a waiting candidate leads once the leader resigns, with
// a term above the term of the leader.
func (su *suite) campaignsAfterResign(t *testing.T) {
	t.Parallel()
	s := su.open(t)
	first, err := s.Campaign(t.Context(), scope)
	assert.NoError(t, err, "Campaign grants a term")
	type grant struct {
		l   store.Leadership
		err error
	}
	grants := make(chan grant, 1)
	go func() {
		l, err := s.Campaign(t.Context(), scope)
		grants <- grant{l: l, err: err}
	}()
	assert.NoError(t, first.Resign(t.Context()), "Resign returns nil")
	streamtest.Receive(t, first.Done(), waitFor, "Done is closed once the leader resigns")
	next, _ := streamtest.Receive(t, grants, waitFor, "the next candidate leads within the wait of the suite")
	assert.NoError(t, next.err, "Campaign grants the next candidate a term")
	assert.True(t, next.l.Term() > first.Term(), "the next term is above the term before it")
	assert.NoError(t, next.l.Resign(t.Context()), "Resign returns nil")
}

// fitsLaws runs the law of Fits.
func (su *suite) fitsLaws(t *testing.T) {
	t.Parallel()
	t.Run("returns ErrTooLarge for exactly the mutations that Commit refuses as too large", func(t *testing.T) {
		t.Parallel()
		prop.ForAll(t, "Fits returns ErrTooLarge for each mutation that Commit refuses as too large, and nil "+
			"for each mutation that Commit applies", func(c *prop.Case) {
			s := su.open(c)
			lim := s.Limits(scope)
			n := c.Draw(prop.Integer(0, min(lim.MaxPlacements, maxFitsPlacements)+fitsPlacementSlack), "placements")
			size := c.Draw(prop.Integer(0, min(lim.MaxBytes, maxFitsBytes)+fitsByteSlack), "spec bytes")
			m := store.Mutation{
				ID:   store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1},
				Put:  make([]shard.Placement, n),
				Spec: make([]byte, size),
			}
			for i := range m.Put {
				m.Put[i] = shard.Placement{
					Shard: shard.ID{ID: id.New128([16]byte{0: tagFits, 14: byte(i >> 8), 15: byte(i)})},
					Range: shard.Range{Start: []byte{byte(i >> 8), byte(i)}},
				}
			}
			fits := s.Fits(scope, &m)
			_, committed := s.Writer(scope, incA, lawTerm).Commit(c.Context(), 0, &m)
			if fits != nil {
				assert.ErrorIs(c, fits, store.ErrTooLarge, "Fits returns ErrTooLarge for a mutation that does not fit")
				assert.ErrorIs(
					c,
					committed,
					store.ErrTooLarge,
					"Commit returns ErrTooLarge for a mutation that Fits refuses",
				)
				h, err := s.Head(c.Context(), scope)
				assert.NoError(c, err, "Head returns the header of the scope")
				assert.Equal(c, h, store.Head{}, "a commit of a mutation that does not fit leaves the scope unchanged")
				return
			}
			assert.NoError(c, committed, "Commit applies a mutation that fits")
		})
	})
}

// closeLaws runs the law of Close.
func (su *suite) closeLaws(t *testing.T) {
	t.Parallel()
	t.Run("ends the watches and the terms of the store", func(t *testing.T) {
		t.Parallel()
		s := su.newStore(t)
		cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
		cm.commit(t, &store.Mutation{})
		l, err := s.Campaign(t.Context(), scope)
		assert.NoError(t, err, "Campaign grants a term")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
		_, ok := streamtest.Receive(
			t,
			items,
			waitFor,
			"Watch yields the generation of the scope within the wait of the suite",
		)
		assert.True(t, ok, "Watch yields the generation of the scope")
		assert.NoError(t, s.Close(), "Close returns nil")
		streamtest.Receive(t, l.Done(), waitFor, "Done of a term is closed once its store closes")
		streamtest.End(t, items, waitFor, "a watch ends once its store closes")
	})
}

// AssertStore checks the laws of package store against the stores that newStore returns.
// newStore returns an empty store, and fails tb when it cannot open one. Each case of a law
// opens a store of its own, so no case observes the scopes of another, and the suite
// closes each store when its case ends. A case can call newStore on a goroutine other than
// the goroutine of the test, with an assert.TB that is not a *testing.T.
//
// The suite runs one subtest per method of the store:
//
//   - Head returns the zero Head of a scope before its first commit.
//   - Read returns the zero State of such a scope, the state that generated commits leave,
//     and pages that list each selected placement once at the generation of the read. It
//     returns shard.ErrIncarnation for a scope of another incarnation.
//   - Watch yields each later generation once with its whole mutation, each change that
//     concerns a node, and each commit after its start. It ends with its context, and
//     yields shard.ErrIncarnation for a scope of another incarnation.
//   - Writer and Commit pass core's fence laws, and a machine of commits on two clients
//     checks after every step that Commit, Head, Read and Outcome are linearizable against
//     a model of the scope. A mutation that does not encode leaves the scope unchanged.
//   - Campaign grants each term to one candidate at a time, above the watermark and the
//     terms before it. A term ends with the context of its campaign, and the next
//     candidate leads once the leader resigns. Campaign returns the error of its context
//     while another candidate leads.
//   - Fits returns store.ErrTooLarge for exactly the mutations that Commit refuses as too
//     large, near the limits of the store.
//   - Close ends the watches and the terms of the store.
//
// The suite commits mutations of up to 3 placements, 3 transitions, 3 reservations and 3
// drains, so the limits of the store admit them. Each [Option] adds the laws that its
// docblock lists.
//
// # Concurrency
//
// The subtests of the suite run in parallel.
func AssertStore(t *testing.T, newStore func(tb assert.TB) store.Store, opts ...Option) {
	t.Helper()
	su := &suite{newStore: newStore, fx: newFixture(), tab: newTable(t)}
	for _, o := range opts {
		o(&su.cfg)
	}
	t.Run("Head", su.headLaws)
	t.Run("Read", su.readLaws)
	t.Run("Watch", su.watchLaws)
	t.Run("Writer", su.writerLaws)
	t.Run("Outcome", su.outcomeLaws)
	t.Run("Campaign", su.campaignLaws)
	t.Run("Fits", su.fitsLaws)
	t.Run("Close", su.closeLaws)
}

// generateCommit generates the input of a commit of the machine of commits. The input has a
// writer, a number, a variant and the generation that the commit expects.
func generateCommit(c *prop.Case, _ *scopeModel) any {
	return commitCall{
		w:       c.Draw(prop.Integer(0, len(writers)-1), "writer"),
		seq:     c.Draw(prop.Integer(1, seqs), "number"),
		variant: c.Draw(prop.Integer(0, variants-1), "variant"),
		expect:  c.Draw(prop.Integer[epoch.Epoch](0, expects), "expect"),
	}
}

// generateOutcome generates the input of a call of Outcome of the machine of commits. The
// input has a writer and a number.
func generateOutcome(c *prop.Case, _ *scopeModel) any {
	return outcomeCall{
		w:   c.Draw(prop.Integer(0, len(writers)-1), "writer"),
		seq: c.Draw(prop.Integer(1, seqs), "number"),
	}
}

// generateLoss generates the point at which the next commit of the machine of commits fails.
func generateLoss(c *prop.Case, _ *scopeModel) any {
	return c.Draw(prop.SampledFrom(LoseRequest, LoseReply), "loss")
}

// selected returns the placements that a Read of node returns after the commits of
// batches: the last placement of each shard, with a replica on node unless node is empty,
// in the order of the starts of their ranges.
func selected(batches [][]shard.Placement, node shard.NodeID) []shard.Placement {
	last := make(map[shard.ID]*shard.Placement)
	for _, b := range batches {
		for i := range b {
			last[b[i].Shard] = &b[i]
		}
	}
	want := make([]shard.Placement, 0, len(last))
	for _, p := range last {
		if node == "" || slices.ContainsFunc(p.Replicas, func(r shard.Replica) bool { return r.Node == node }) {
			want = append(want, *p)
		}
	}
	slices.SortFunc(want, func(a, b shard.Placement) int { return bytes.Compare(a.Range.Start, b.Range.Start) })
	return want
}

// drain returns the placements of the page of cur and the token of the next page, and
// closes cur. It fails tb when cur yields an error or more than limit placements.
func drain(tb assert.TB, cur page.Cursor[shard.Placement], limit int) ([]shard.Placement, string) {
	tb.Helper()
	var placements []shard.Placement
	for p, err := range cur.Seq(assert.Context(tb)) {
		assert.NoError(tb, err, "the cursor of Read yields the placements of its page")
		placements = append(placements, p)
	}
	assert.InRange(tb, len(placements), 0, float64(limit), "a page has at most the limit of its request")
	token := cur.NextPage()
	assert.NoError(tb, cur.Close(), "Close of a cursor returns nil")
	return placements, token
}

// capture returns the image of the scope that s returns: its header, its state, its
// placements on one page, and the receipts of the commits of cm.
func capture(tb assert.TB, s store.Store, cm *committer) image {
	tb.Helper()
	ctx := assert.Context(tb)
	var im image
	var err error
	im.head, err = s.Head(ctx, scope)
	assert.NoError(tb, err, "Head returns the header of the scope")
	state, cur, err := s.Read(ctx, scope, incA, store.Query{}, page.Page{Limit: shardCount})
	assert.NoError(tb, err, "Read returns the state of the scope")
	im.state = state
	var token string
	im.placements, token = drain(tb, cur, shardCount)
	assert.Equal(tb, token, "", "Read returns every placement of the scope on one page of its limit")
	for seq := range cm.seq {
		r, ok, err := s.Outcome(ctx, scope, store.CommitID{Incarnation: cm.inc, Term: lawTerm, Seq: seq + 1})
		assert.NoError(tb, err, "Outcome returns the receipt of a commit")
		assert.True(tb, ok, "the scope keeps the receipt of each commit that no commit confirmed")
		im.receipts = append(im.receipts, r)
	}
	return im
}

// sameChange fails tb when the event got of a watch of a node differs from want: another
// generation, commit ID or controller record, other placements, transitions, ended shards
// or removed shards in any order, or another change of the mutation.
func sameChange(tb assert.TB, got, want *store.Event) {
	tb.Helper()
	assert.Equal(tb, got.Generation, want.Generation, "Watch yields each generation with a change of the node")
	assert.Permutation(tb, got.Mutation.Put, want.Mutation.Put,
		"an event of a node lists each placement that names the node or named it before", assert.EquateEmpty())
	assert.Permutation(tb, got.Mutation.Transitions, want.Mutation.Transitions,
		"an event of a node lists each transition with an endpoint on the node", assert.EquateEmpty())
	assert.Permutation(tb, got.Mutation.Done, want.Mutation.Done,
		"an event of a node lists each ended transition with an endpoint on the node", assert.EquateEmpty())
	assert.Permutation(tb, got.Removed, want.Removed,
		"an event of a node lists each shard whose placement stops naming the node", assert.EquateEmpty())
	rest := got.Mutation
	rest.Put, rest.Transitions, rest.Done = nil, nil, nil
	assert.Equal(tb, rest, store.Mutation{ID: want.Mutation.ID, Cell: want.Mutation.Cell},
		"an event of a node contains the ID of its commit and no change apart from the changes of the node",
		assert.EquateEmpty())
}

// endsWith checks that a watch by s from the generation from, for the incarnation inc,
// yields an error that wraps target and then ends.
func endsWith(t *testing.T, s store.Store, inc shard.NamespaceIncarnation, from epoch.Epoch, target error,
	msg string,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, inc, from, ""))
	it, ok := streamtest.Receive(t, items, waitFor, "Watch yields an error within the wait of the suite")
	assert.True(t, ok, "Watch yields an error before it ends")
	assert.ErrorIs(t, it.Err, target, msg)
	streamtest.End(t, items, waitFor, "Watch ends after its error")
}

// until returns the events of items up to the first event at the generation last, for a
// watch from the generation from. It fails tb when items yields an error, ends first, or
// yields more than one event per generation.
func until(tb assert.TB, items <-chan streamtest.Item[store.Event], from, last epoch.Epoch) []store.Event {
	tb.Helper()
	events := make([]store.Event, 0, last-from)
	for range last - from {
		it, ok := streamtest.Receive(tb, items, waitFor, "Watch yields an event within the wait of the suite")
		assert.True(tb, ok, "Watch yields every committed generation before its context ends")
		assert.NoError(tb, it.Err, "Watch yields the generations of the scope")
		events = append(events, it.Value)
		if it.Value.Generation >= last {
			break
		}
	}
	assert.Equal(tb, events[len(events)-1].Generation, last,
		"Watch yields one event at most per generation, up to the last committed generation")
	return events
}
