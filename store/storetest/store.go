// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package storetest

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/expect"
	"go.dokimi.dev/assert/history"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/assert/stateful"
	"go.thesmos.sh/core/clock/fake"
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
	"go.thesmos.sh/sharder/store/memory"
)

// The names in the records of the suite.
const (
	// namespace is the namespace of every scope of the suite.
	namespace shard.Namespace = "storetest"
	// cell is the cell of the scope that the checks commit to.
	cell shard.CellID = "cell-1"
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
	// maxCommits is the most mutations that one case of a check commits.
	maxCommits = 5
	// maxLimit is the largest limit of a page of the check of pages.
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
	// candidates is the number of clients of the check of terms.
	candidates = 3
	// rounds is the number of terms that each client of the check of terms leads.
	rounds = 3
	// maxFitsPlacements is the most placements of a mutation of the check of Fits below
	// the limit of the store.
	maxFitsPlacements = 32
	// maxFitsBytes is the most bytes of the spec of a mutation of the check of Fits below
	// the limit of the store.
	maxFitsBytes = 8192
	// fitsPlacementSlack is how many placements a mutation of the check of Fits puts
	// beyond the limit of the store.
	fitsPlacementSlack = 2
	// fitsByteSlack is how many bytes the spec of a mutation of the check of Fits has
	// beyond the limit of the store.
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
	// lawTerm is the term of the writers of the checks apart from the machine of commits.
	lawTerm epoch.Epoch = 1
	// highTerm is the term of the commit that raises the watermark of the check of terms.
	highTerm epoch.Epoch = 7
	// invalidPhase is a phase that is not valid, so a transition with it does not encode.
	invalidPhase = shard.PhaseAborted + 1
)

// waitFor bounds each wait of the suite for an event, a term or the end of a watch that a
// correct store delivers at once.
const waitFor = 10 * time.Second

// The identities and the instants of the suite.
var (
	// incA is the namespace incarnation of the writers of the checks.
	incA = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0a})}
	// incB is the namespace incarnation of the writers of the checks of another
	// incarnation.
	incB = shard.NamespaceIncarnation{ID: id.New128([16]byte{0: tagIncarnation, 15: 0x0b})}
	// scope is the scope that the checks commit to.
	scope = shard.Scope{Namespace: namespace, Cell: cell}
	// root is the root scope of the namespace of scope.
	root = shard.Scope{Namespace: namespace}
	// origin is the first instant of the records of the suite, and the instant of the
	// clock of the memory stores of the planted defects.
	origin = time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
	// limits are the limits of the memory stores of the planted defects. The check of Fits
	// generates mutations on both sides of them, and every other check commits mutations
	// below them.
	limits = store.Limits{MaxPlacements: 8, MaxBytes: 4096}
	// lossPoints maps each loss to the point of a memory store that loses a commit there.
	lossPoints = map[Loss]memory.Point{LoseRequest: memory.PointCommitApply, LoseReply: memory.PointCommitAck}
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
	// fx is the fixture of the checks.
	fx = newFixture()
	// tables returns the table of the machine of commits, which the first call builds.
	tables = sync.OnceValues(newTable)
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

// Loss is the point at which a fault that [Faulty.LoseCommit] injects interrupts a commit.
//
// [Loss.IsValid] refuses the zero Loss.
//
// # Concurrency
//
// A Loss is a comparable value.
//
//dokimi:enum text=comment
type Loss uint8

// The points at which a fault interrupts a commit.
const (
	// LoseRequest loses a commit before the store applies it, so the commit does not apply.
	LoseRequest Loss = 1 // request
	// LoseReply loses the reply of a commit after the store applies it, so the commit and
	// its receipt remain.
	LoseReply Loss = 2 // reply
)

// Faulty is a store whose next commit a fault interrupts. The checks of a commit whose
// outcome is unknown call it, and the machine of commits loses commits through it.
type Faulty interface {
	// LoseCommit makes the next commit to the store fail with an error that wraps
	// version.ErrOutcomeUnknown, at the point that l selects.
	LoseCommit(l Loss)
}

// Deleter is a store that deletes a scope, as an operator does before a namespace is
// created again under the same name. The checks of a scope that another incarnation
// creates again call it.
type Deleter interface {
	// Delete removes the scope sc from the storage of the store. A later commit at the
	// generation zero creates the scope again.
	Delete(sc shard.Scope)
}

// Compactor is a store that discards the old generations of its scopes, as the compaction
// of a backend does. The checks of a discarded generation call it.
type Compactor interface {
	// Compact discards the generations of every scope of the store below their current
	// generations.
	Compact()
}

// fixture contains the identities of the records of the suite, and the generators of its
// mutations.
type fixture struct {
	// puts generates the placements of one commit.
	puts prop.Generator[[]shard.Placement]
	// mutations generates the mutations that one case of a check commits, in order.
	mutations prop.Generator[[]store.Mutation]
	// ranges lists the range of each shard of shards.
	ranges [shardCount]shard.Range
	// everywhere lists a placement of each shard with a replica on each node.
	everywhere []shard.Placement
	// mapping is the mapping of the namespace, which the first commit of the check of the
	// state records.
	mapping shard.Mapping
	// cell is the controller record that the last commit of the check of node watches
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

// newFixture returns the fixture of the suite. The shards cover the positions in order,
// the first from the empty start and the last to the end of the position space.
func newFixture() *fixture {
	f := &fixture{
		mapping: shard.Mapping{Partitioner: partitioner, Config: []byte(`{"slots":16}`)},
		cell:    shard.Cell{ID: cell, Controller: controller, ControllerIdentity: caller, Epoch: lawTerm},
	}
	for i := range shardCount {
		f.shards[i] = shard.ID{ID128: id.ID128{0: tagShard, 15: byte(i + 1)}}
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

// generateMutation generates one mutation of the checks over generated commits. The
// mutation has placements, transitions, ended shards, applied and forgotten proposals,
// reserved and released disruptions, drains and undrains, a spec and a controller record.
// Each list contains a shard, a proposal, a disruption or a drain at most once.
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
// its writer before it.
//
// Error modes:
//   - the error of Mutation.Digest for a mutation that does not encode.
func newTable() (*table, error) {
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
				if err != nil {
					return nil, fmt.Errorf("storetest: the digest of the commit %v: %w", m.ID, err)
				}
				tab.digests[w][s][v] = d
			}
		}
	}
	return tab, nil
}

// machine is the subject of the machine of commits in one case. It has a store, the scope
// of its calls, the table of their mutations, and the count of the commits that it lost.
type machine struct {
	// s is the store of the case.
	s store.Store
	// lose is the store of the case when it implements Faulty, and nil otherwise.
	lose Faulty
	// tab is the table of the mutations of the commits.
	tab *table
	// sc is the scope of the calls.
	sc shard.Scope
	// lost counts the calls of loseNext in the case.
	lost atomic.Int32
}

// actions returns the actions of the machine. They call Head, Read and Outcome, Commit at
// an arbitrary generation, and Commit at the generation that Head returns. A store that
// implements Faulty adds an action that loses the next commit.
func (m *machine) actions() []stateful.Action[struct{}] {
	actions := []stateful.Action[struct{}]{
		{Name: opHead, Run: func(c *prop.Case, client int, _ any) { m.head(c, client) }},
		{Name: opRead, Run: m.read},
		{Name: opOutcome, Input: generateOutcome, Run: m.outcome},
		{Name: opCommit, Input: generateCommit, Run: m.commitAt},
		{Name: opCommitAtHead, Input: generateCommit, Run: m.commitAtHead},
	}
	if m.lose != nil {
		actions = append(actions, stateful.Action[struct{}]{Name: opLose, Input: generateLoss, Run: m.loseNext})
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
func (m *machine) loseNext(_ *prop.Case, _ int, in any) {
	if m.lost.Add(1) > maxLost {
		return
	}
	l, _ := in.(Loss)
	m.lose.LoseCommit(l)
}

// lease is the state of the election of a scope in the check of terms: the last term that
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

// committer commits the mutations of one check to the scope of a store, in order, each at
// the generation before it, through a writer of one incarnation at the term lawTerm.
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
// mutations of a check leave it.
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
			return a.Operation.Shard.Compare(b.Operation.Shard)
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

// watchModel is the placements and the transitions of a scope, as the mutations of a check
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

// backend is the memory store under a planted defect of the suite, the verified fake of the
// planted defects. It records the scopes that a planted store deleted, and whether a
// planted store compacted it.
//
// # Concurrency
//
// A backend is safe for concurrent use.
type backend struct {
	*memory.Store
	// deleted contains each scope that Delete removed.
	deleted map[shard.Scope]bool
	mu      sync.Mutex
	// compacted reports a call of Compact.
	compacted bool
}

// newBackend returns an empty memory store with the limits of the suite, and closes it when
// the check of h ends.
func newBackend(h *StoreHandle) *backend {
	b := &backend{
		Store:   memory.New(fake.New(origin), memory.WithLimits(limits)),
		deleted: make(map[shard.Scope]bool),
	}
	h.Cleanup(func() { assert.NoError(h, b.Close(), "Close of the memory store returns nil") })
	return b
}

// wasDeleted reports whether a planted store deleted sc.
func (b *backend) wasDeleted(sc shard.Scope) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleted[sc]
}

// wasCompacted reports whether a planted store compacted b.
func (b *backend) wasCompacted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.compacted
}

// plantedStore is a planted defect of the suite: the double of store.Store over a backend,
// with the operations of the kit on the backend.
//
// # Concurrency
//
// A plantedStore is safe for concurrent use.
type plantedStore struct {
	store.Store
	// b is the backend of the double.
	b *backend
	// points maps each loss to the point of the backend at which LoseCommit arms a fault.
	points map[Loss]memory.Point
}

// The operations of the kit that a planted store implements.
var (
	_ Faulty    = plantedStore{}
	_ Deleter   = plantedStore{}
	_ Compactor = plantedStore{}
)

// plant returns the double of the store b, with the options opts that replace the calls of
// b that break a law. LoseCommit of the double arms the faults of lossPoints.
func plant(h *StoreHandle, b *backend, opts ...StoreStubOption) plantedStore {
	return plantedStore{
		Store:  NewStoreStub(h, append([]StoreStubOption{StoreStubDelegateTo(b.Store)}, opts...)...),
		b:      b,
		points: lossPoints,
	}
}

// LoseCommit arms the fault of the backend at the point of l.
func (s plantedStore) LoseCommit(l Loss) {
	s.b.FailAt(s.points[l])
}

// Delete records sc, and removes it from the backend.
func (s plantedStore) Delete(sc shard.Scope) {
	s.b.mu.Lock()
	s.b.deleted[sc] = true
	s.b.mu.Unlock()
	s.b.Delete(sc)
}

// Compact records the compaction, and discards the old generations of the backend.
func (s plantedStore) Compact() {
	s.b.mu.Lock()
	s.b.compacted = true
	s.b.mu.Unlock()
	s.b.Compact()
}

// HeadsStartAtZero checks that Head returns the zero Head of a scope before its first
// commit, also while the root scope of its namespace has commits.
//
//dokimi:check Store.Head defect=headsFromTheRoot
func HeadsStartAtZero(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	commitsToTheRoot(h, c, s)
	got, err := s.Head(h.Context(), scope)
	assert.NoError(h, err, h.Statement())
	assert.Equal(h, got, store.Head{}, h.Statement())
}

// headsFromTheRoot returns a planted defect whose Head returns the header of the root scope
// of the namespace for a cell scope before its first commit.
func headsFromTheRoot(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreHead(func(ctx context.Context, s shard.Scope) (store.Head, error) {
		got, err := b.Head(ctx, s)
		if err != nil || got.Generation > 0 || s.Cell == "" {
			return got, err
		}
		return b.Head(ctx, shard.Scope{Namespace: s.Namespace})
	}))
}

// ReadsStartEmpty checks that Read returns the zero State and an empty page for a scope
// before its first commit, also while the root scope of its namespace has commits.
//
//dokimi:check Store.Read defect=readsFromTheRoot
func ReadsStartEmpty(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	commitsToTheRoot(h, c, s)
	inc := c.Draw(prop.SampledFrom(shard.NamespaceIncarnation{}, incA, incB), "incarnation")
	state, cur, err := s.Read(h.Context(), scope, inc, store.Query{}, page.Page{Limit: maxLimit})
	assert.NoError(h, err, h.Statement())
	placements, token := drain(h, cur, maxLimit)
	assert.Equal(h, state, store.State{}, h.Statement(), assert.EquateEmpty())
	assert.Empty(h, placements, h.Statement())
	assert.Equal(h, token, "", h.Statement())
}

// readsFromTheRoot returns a planted defect whose Read returns the state and the
// placements of the root scope of the namespace for a cell scope before its first commit.
func readsFromTheRoot(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreRead(
		func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q store.Query, p page.Page) (
			store.State, page.Cursor[shard.Placement], error,
		) {
			head, err := b.Head(ctx, s)
			if err != nil || head.Generation > 0 || s.Cell == "" {
				return b.Read(ctx, s, inc, q, p)
			}
			return b.Read(ctx, shard.Scope{Namespace: s.Namespace}, inc, q, p)
		}))
}

// ReadsReturnTheCommittedState checks that Read returns the state that the commits to a
// scope leave.
//
//dokimi:check Store.Read defect=forgetsDrains
func ReadsReturnTheCommittedState(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	ms := c.Draw(fx.mutations, "mutations")
	ms[0].Mapping = &fx.mapping
	sm := newStateModel()
	for i := range ms {
		cm.commit(h, &ms[i])
		sm.apply(&ms[i])
	}
	got, cur, err := s.Read(h.Context(), scope, incA, store.Query{}, page.Page{})
	assert.NoError(h, err, h.Statement())
	assert.NoError(h, cur.Close(), "Close of a cursor returns nil")
	want := sm.state(store.Head{Generation: cm.gen, Watermark: lawTerm, Schema: store.Schema, Incarnation: incA})
	assert.Equal(h, got, want, h.Statement(), assert.EquateEmpty())
}

// forgetsDrains returns a planted defect whose Read returns each state without its drains.
func forgetsDrains(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreRead(
		func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q store.Query, p page.Page) (
			store.State, page.Cursor[shard.Placement], error,
		) {
			state, cur, err := b.Read(ctx, s, inc, q, p)
			state.Drains = nil
			return state, cur, err
		}))
}

// PagesListEachSelectedPlacementOnce checks that the pages of a Read list each selected
// placement once, in position order, at the generation of the read.
//
//dokimi:check Store.Read defect=ignoresNodes
func PagesListEachSelectedPlacementOnce(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	batches := c.Draw(prop.List(fx.puts, prop.MinSize(1), prop.MaxSize(maxCommits)), "batches")
	for _, b := range batches {
		cm.commit(h, &store.Mutation{Put: b})
	}
	limit := c.Draw(prop.Integer(1, maxLimit), "limit")
	node := c.Draw(prop.SampledFrom[shard.NodeID]("", nodeA, nodeB, nodeC), "node")
	between := c.Draw(prop.Boolean(), "commit between the pages")
	q := store.Query{Node: node}
	first, cur, err := s.Read(h.Context(), scope, incA, q, page.Page{Limit: limit})
	assert.NoError(h, err, h.Statement())
	got, token := drain(h, cur, limit)
	if between {
		cm.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere)})
	}
	for later := 1; token != ""; later++ {
		assert.True(h, later <= shardCount, h.Statement())
		state, next, err := s.Read(h.Context(), scope, incA, q, page.Page{Token: token, Limit: limit})
		c.Assume(!errors.Is(err, store.ErrCompacted))
		assert.NoError(h, err, h.Statement())
		assert.Equal(h, state.Head.Generation, first.Head.Generation, h.Statement())
		var placements []shard.Placement
		placements, token = drain(h, next, limit)
		got = append(got, placements...)
	}
	assert.Equal(h, got, selected(batches, node), h.Statement(), assert.EquateEmpty())
}

// ignoresNodes returns a planted defect whose Read returns every placement of a scope,
// whatever node its query selects.
func ignoresNodes(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreRead(
		func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, _ store.Query, p page.Page) (
			store.State, page.Cursor[shard.Placement], error,
		) {
			return b.Read(ctx, s, inc, store.Query{}, p)
		}))
}

// ReadsRefuseOtherIncarnations checks that Read returns ErrIncarnation for a scope that
// another incarnation created.
//
//dokimi:check Store.Read defect=readsOtherIncarnations
func ReadsRefuseOtherIncarnations(h *StoreHandle) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	_, _, err := s.Read(h.Context(), scope, incB, store.Query{}, page.Page{})
	assert.ErrorIs(h, err, shard.ErrIncarnation, h.Statement())
}

// readsOtherIncarnations returns a planted defect whose Read without a page token returns
// the state of a scope of another incarnation in place of ErrIncarnation.
func readsOtherIncarnations(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreRead(
		func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q store.Query, p page.Page) (
			store.State, page.Cursor[shard.Placement], error,
		) {
			state, cur, err := b.Read(ctx, s, inc, q, p)
			if errors.Is(err, shard.ErrIncarnation) && p.Token == "" {
				return b.Read(ctx, s, shard.NamespaceIncarnation{}, q, p)
			}
			return state, cur, err
		}))
}

// ReadsRefuseRecreatedScopes checks that Read returns ErrIncarnation for a scope that
// another incarnation created again.
//
//dokimi:check Store.Read defect=readsOtherIncarnations
func ReadsRefuseRecreatedScopes(h *StoreHandle) {
	if _, ok := recreate(h); !ok {
		return
	}
	_, _, err := h.Subject().Read(h.Context(), scope, incA, store.Query{}, page.Page{})
	assert.ErrorIs(h, err, shard.ErrIncarnation, h.Statement())
}

// PagesRefuseRecreatedScopes checks that Read returns ErrIncarnation for a page token of an
// incarnation whose scope another incarnation created again.
//
//dokimi:check Store.Read defect=restartsPagesOfOtherIncarnations
func PagesRefuseRecreatedScopes(h *StoreHandle) {
	token, ok := recreate(h)
	if !ok {
		return
	}
	_, _, err := h.Subject().Read(
		h.Context(),
		scope,
		shard.NamespaceIncarnation{},
		store.Query{},
		page.Page{Token: token, Limit: 1},
	)
	assert.ErrorIs(h, err, shard.ErrIncarnation, h.Statement())
}

// restartsPagesOfOtherIncarnations returns a planted defect whose Read of a page token of
// another incarnation returns the first page of the scope in place of ErrIncarnation.
func restartsPagesOfOtherIncarnations(h *StoreHandle) store.Store {
	return restartsPages(h, shard.ErrIncarnation)
}

// PagesRefuseCompactedGenerations checks that Read returns ErrCompacted for a page token of
// a discarded generation.
//
//dokimi:check Store.Read defect=restartsCompactedPages
func PagesRefuseCompactedGenerations(h *StoreHandle) {
	compactor, ok := operation[Compactor](h)
	if !ok {
		return
	}
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere[:2])})
	_, cur, err := s.Read(h.Context(), scope, incA, store.Query{}, page.Page{Limit: 1})
	assert.NoError(h, err, "Read returns the first page of the scope")
	_, token := drain(h, cur, 1)
	cm.commit(h, &store.Mutation{Spec: []byte(`{}`)})
	compactor.Compact()
	_, _, err = s.Read(h.Context(), scope, incA, store.Query{}, page.Page{Token: token, Limit: 1})
	assert.ErrorIs(h, err, store.ErrCompacted, h.Statement())
}

// restartsCompactedPages returns a planted defect whose Read of a page token of a discarded
// generation returns the first page of the scope in place of ErrCompacted.
func restartsCompactedPages(h *StoreHandle) store.Store {
	return restartsPages(h, store.ErrCompacted)
}

// ReadsSurviveRestarts checks that a store that opens again after a restart returns the
// header, the state, the placements and the receipts of the commits before the restart.
//
//dokimi:check Store.Read defect=forgetsOnRestart restart=forget
func ReadsSurviveRestarts(h *StoreHandle) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere[:3])})
	cm.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere[3:]), Spec: []byte(`{}`)})
	before := capture(h, s, cm)
	assert.NotContains(h, before.receipts, store.Receipt{}, "the scope keeps the receipt of each commit")
	assert.Equal(h, capture(h, h.Restart(s), cm), before, h.Statement(), assert.EquateEmpty())
}

// forgetsOnRestart returns the planted defect of ReadsSurviveRestarts: a planted store over
// a memory store of its own, which forget replaces with an empty planted store.
func forgetsOnRestart(h *StoreHandle) store.Store {
	return plant(h, newBackend(h))
}

// forget returns an empty planted store in place of the planted defect of
// ReadsSurviveRestarts after a restart.
func forget(h *StoreHandle, _ store.Store) store.Store {
	return plant(h, newBackend(h))
}

// WatchesYieldEachGenerationOnce checks that Watch yields each later generation once, in
// order, with its whole mutation.
//
//dokimi:check Store.Watch defect=yieldsHistoryWithoutSpecs
func WatchesYieldEachGenerationOnce(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	ms := c.Draw(fx.mutations, "mutations")
	for i := range ms {
		cm.commit(h, &ms[i])
	}
	from := c.Draw(prop.Integer(0, len(ms)-1), "from")
	inc := c.Draw(prop.SampledFrom(shard.NamespaceIncarnation{}, incA), "incarnation")
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, inc, epoch.Epoch(from), ""))
	events := until(h, items, epoch.Epoch(from), cm.gen)
	assert.Length(h, events, len(ms)-from, h.Statement())
	for i := range events {
		e := &events[i]
		assert.Equal(h, e.Generation, epoch.Epoch(from+i+1), h.Statement())
		assert.Equal(h, e.Mutation, ms[from+i], h.Statement(), assert.EquateEmpty())
		assert.Empty(h, e.Removed, h.Statement())
	}
}

// yieldsHistoryWithoutSpecs returns a planted defect whose watch without a node yields the
// generations up to its start without their specs.
func yieldsHistoryWithoutSpecs(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		events := b.Watch(ctx, s, inc, from, node)
		head, err := b.Head(ctx, s)
		if err != nil || node != "" {
			return events
		}
		return func(yield func(store.Event, error) bool) {
			for e, err := range events {
				if e.Generation <= head.Generation {
					e.Mutation.Spec = nil
				}
				if !yield(e, err) {
					return
				}
			}
		}
	}))
}

// NodeWatchesYieldTheirChanges checks that a Watch of a node yields each change that
// concerns the node.
//
//dokimi:check Store.Watch defect=dropsRemovals
func NodeWatchesYieldTheirChanges(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	ms := c.Draw(fx.mutations, "mutations")
	ms[len(ms)-1].Cell = &fx.cell
	node := c.Draw(nodes, "node")
	wm := newWatchModel()
	var want []store.Event
	for i := range ms {
		g := cm.commit(h, &ms[i])
		if e, ok := wm.event(&ms[i], g, node); ok {
			want = append(want, e)
		}
	}
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	events := until(h, streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, node)), 0, cm.gen)
	assert.Pairwise(
		h,
		events,
		func(earlier, later store.Event) bool { return earlier.Generation < later.Generation },
		h.Statement(),
	)
	var changes []store.Event
	for i := range events {
		e := &events[i]
		if e.Mutation.ID != (store.CommitID{}) {
			changes = append(changes, *e)
			continue
		}
		assert.Equal(h, e.Mutation, store.Mutation{}, h.Statement(), assert.EquateEmpty())
		assert.Empty(h, e.Removed, h.Statement())
	}
	assert.Length(h, changes, len(want), h.Statement())
	for i := range want {
		sameChange(h, &changes[i], &want[i])
	}
}

// dropsRemovals returns a planted defect whose watch of a node yields each event without
// the shards whose placements stop naming the node.
func dropsRemovals(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		events := b.Watch(ctx, s, inc, from, node)
		return func(yield func(store.Event, error) bool) {
			for e, err := range events {
				e.Removed = nil
				if !yield(e, err) {
					return
				}
			}
		}
	}))
}

// WatchesFollowCommits checks that Watch yields each commit that applies after the watch
// started, with its whole mutation.
//
//dokimi:check Store.Watch defect=stripsLaterMutations
func WatchesFollowCommits(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	ms := c.Draw(fx.mutations, "mutations")
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
	for i := range ms {
		g := cm.commit(h, &ms[i])
		it, ok := streamtest.Receive(h, items, waitFor, h.Statement())
		assert.True(h, ok, h.Statement())
		assert.NoError(h, it.Err, h.Statement())
		assert.Equal(h, it.Value.Generation, g, h.Statement())
		assert.Equal(h, it.Value.Mutation, ms[i], h.Statement(), assert.EquateEmpty())
	}
}

// stripsLaterMutations returns a planted defect whose watch without a node yields each
// generation after its start without its mutation.
func stripsLaterMutations(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		events := b.Watch(ctx, s, inc, from, node)
		head, err := b.Head(ctx, s)
		if err != nil || node != "" {
			return events
		}
		return func(yield func(store.Event, error) bool) {
			for e, err := range events {
				if e.Generation > head.Generation {
					e.Mutation = store.Mutation{}
				}
				if !yield(e, err) {
					return
				}
			}
		}
	}))
}

// WatchesEndWithTheirContext checks that Watch ends once its context ends.
//
//dokimi:check Store.Watch defect=ignoresWatchContexts
func WatchesEndWithTheirContext(h *StoreHandle) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
	_, ok := streamtest.Receive(h, items, waitFor, "Watch yields the generation of the scope within the wait")
	assert.True(h, ok, "Watch yields the generation of the scope")
	cancel()
	streamtest.End(h, items, waitFor, h.Statement())
}

// ignoresWatchContexts returns a planted defect whose watch continues after its context
// ends.
func ignoresWatchContexts(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		return b.Watch(context.WithoutCancel(ctx), s, inc, from, node)
	}))
}

// WatchesRefuseOtherIncarnations checks that Watch yields ErrIncarnation for a scope that
// another incarnation created, and ends.
//
//dokimi:check Store.Watch defect=watchesOtherIncarnations
func WatchesRefuseOtherIncarnations(h *StoreHandle) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	endsWith(h, incB, 0, shard.ErrIncarnation)
}

// watchesOtherIncarnations returns a planted defect whose watch of a scope that another
// incarnation created when the watch starts yields its generations in place of
// ErrIncarnation.
func watchesOtherIncarnations(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		head, err := b.Head(ctx, s)
		if err == nil && !inc.IsZero() && !head.Incarnation.IsZero() && head.Incarnation != inc {
			return b.Watch(ctx, s, shard.NamespaceIncarnation{}, from, node)
		}
		return b.Watch(ctx, s, inc, from, node)
	}))
}

// WatchesRefuseCreationsByOtherIncarnations checks that Watch yields ErrIncarnation when
// another incarnation creates the scope, and ends.
//
//dokimi:check Store.Watch defect=checksIncarnationsOnce
func WatchesRefuseCreationsByOtherIncarnations(h *StoreHandle) {
	s := h.Subject()
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incB, 0, ""))
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	it, ok := streamtest.Receive(h, items, waitFor, h.Statement())
	assert.True(h, ok, h.Statement())
	assert.ErrorIs(h, it.Err, shard.ErrIncarnation, h.Statement())
	streamtest.End(h, items, waitFor, h.Statement())
}

// checksIncarnationsOnce returns a planted defect whose watch of a scope before its first
// commit yields the generations of every incarnation that creates the scope.
func checksIncarnationsOnce(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		head, err := b.Head(ctx, s)
		if err == nil && head.Incarnation.IsZero() {
			return b.Watch(ctx, s, shard.NamespaceIncarnation{}, from, node)
		}
		return b.Watch(ctx, s, inc, from, node)
	}))
}

// WatchesRefuseRecreatedScopes checks that Watch yields ErrIncarnation for a scope that
// another incarnation created again, and ends.
//
//dokimi:check Store.Watch defect=watchesOtherIncarnations
func WatchesRefuseRecreatedScopes(h *StoreHandle) {
	if _, ok := recreate(h); !ok {
		return
	}
	endsWith(h, incA, 1, shard.ErrIncarnation)
}

// WatchesRefuseCompactedGenerations checks that Watch yields ErrCompacted from a discarded
// generation, and ends.
//
//dokimi:check Store.Watch defect=restartsCompactedWatches
func WatchesRefuseCompactedGenerations(h *StoreHandle) {
	compactor, ok := operation[Compactor](h)
	if !ok {
		return
	}
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	cm.commit(h, &store.Mutation{Spec: []byte(`{}`)})
	compactor.Compact()
	endsWith(h, incA, 0, store.ErrCompacted)
}

// restartsCompactedWatches returns a planted defect whose watch from a discarded generation
// yields the generations from the current one in place of ErrCompacted.
func restartsCompactedWatches(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		events := b.Watch(ctx, s, inc, from, node)
		return func(yield func(store.Event, error) bool) {
			for e, err := range events {
				if !errors.Is(err, store.ErrCompacted) {
					if !yield(e, err) {
						return
					}
					continue
				}
				head, err := b.Head(ctx, s)
				if err != nil {
					yield(store.Event{}, err)
					return
				}
				for e, err := range b.Watch(ctx, s, inc, head.Generation-1, node) {
					if !yield(e, err) {
						return
					}
				}
				return
			}
		}
	}))
}

// WatchesContinueAfterCompaction checks that Watch yields the commits after the current
// generation of a compacted scope.
//
//dokimi:check Store.Watch defect=refusesCompactedScopes
func WatchesContinueAfterCompaction(h *StoreHandle) {
	compactor, ok := operation[Compactor](h)
	if !ok {
		return
	}
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	cm.commit(h, &store.Mutation{Spec: []byte(`{}`)})
	compactor.Compact()
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, cm.gen, ""))
	g := cm.commit(h, &store.Mutation{Spec: []byte(`{"version":1}`)})
	it, ok := streamtest.Receive(h, items, waitFor, h.Statement())
	assert.True(h, ok, h.Statement())
	assert.NoError(h, it.Err, h.Statement())
	assert.Equal(h, it.Value.Generation, g, h.Statement())
}

// refusesCompactedScopes returns a planted defect whose every watch after a compaction
// yields ErrCompacted.
func refusesCompactedScopes(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		if !b.wasCompacted() {
			return b.Watch(ctx, s, inc, from, node)
		}
		return func(yield func(store.Event, error) bool) {
			yield(store.Event{}, fmt.Errorf("%w: the generation %d of %v", store.ErrCompacted, from, s))
		}
	}))
}

// HistoriesAreLinearizable checks that Commit, Head, Read and Outcome are linearizable
// against the model of a scope.
//
// The machine of commits calls them on two clients, and loses up to two commits of each
// case through a store that implements Faulty. An application requires a share of the
// cases for each outcome of a commit.
//
//dokimi:check Store.Writer defect=admitsFencedCommits options=machineOptions
func HistoriesAreLinearizable(h *StoreHandle, c *prop.Case) {
	tab, err := tables()
	assert.NoError(h, err, "each mutation of the machine of commits encodes")
	m := &machine{s: h.Subject(), tab: tab, sc: scope}
	m.lose, _ = m.s.(Faulty)
	stateful.Steps(c, stateful.Machine[struct{}]{
		Actions: m.actions(),
		Settle: func(c *prop.Case, _ struct{}) {
			history.Linearizable(h, c.History(), commitSpec, h.Statement())
		},
	}, stateful.Clients(2))
}

// machineOptions returns the least share of the cases of the machine of commits that count
// each outcome of a commit. A store that implements Faulty also loses commits, so its cases
// count unknown outcomes too.
func machineOptions(h *StoreHandle) []prop.Option {
	opts := []prop.Option{
		prop.Require(labelApplied, shareHalf),
		prop.Require(labelIncarnation, shareHalf),
		prop.Require(labelMismatch, shareThird),
		prop.Require(labelFenced, shareThird),
		prop.Require(labelReused, shareQuarter),
		prop.Require(labelReplayed, shareQuarter),
	}
	if _, faulty := h.Subject().(Faulty); faulty {
		opts = append(opts, prop.Require(labelUnknown, shareThird))
	}
	return opts
}

// admitsFencedCommits returns a planted defect whose writers commit at the watermark of
// the scope when their term is below it.
func admitsFencedCommits(h *StoreHandle) store.Store {
	b := newBackend(h)
	writer := func(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) store.Writer {
		return NewWriterStub(h, WithWriterCommit(func(ctx context.Context, expect epoch.Epoch, m *store.Mutation) (
			epoch.Epoch, error,
		) {
			head, err := b.Head(ctx, s)
			if err != nil {
				return 0, err
			}
			return b.Writer(s, inc, max(term, head.Watermark)).Commit(ctx, expect, m)
		}))
	}
	return plant(h, b, WithStoreWriter(writer))
}

// CommitsRefuseUnencodableMutations checks that Commit returns the error of the encoding of
// a mutation that does not encode, and leaves the scope unchanged.
//
//dokimi:check Store.Writer defect=appliesUnencodableMutations
func CommitsRefuseUnencodableMutations(h *StoreHandle) {
	s := h.Subject()
	m := store.Mutation{
		ID: store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1},
		Transitions: []store.Transition{{
			Operation: shard.OperationID{Incarnation: incA, Shard: fx.shards[0], Sequence: 1},
			Phase:     invalidPhase,
		}},
	}
	_, err := s.Writer(scope, incA, lawTerm).Commit(h.Context(), 0, &m)
	assert.ErrorIs(h, err, shard.ErrUnknownPhase, h.Statement())
	got, err := s.Head(h.Context(), scope)
	assert.NoError(h, err, "Head returns the header of the scope")
	assert.Equal(h, got, store.Head{}, h.Statement())
}

// appliesUnencodableMutations returns a planted defect whose writers commit a mutation that
// does not encode without its transitions.
func appliesUnencodableMutations(h *StoreHandle) store.Store {
	b := newBackend(h)
	writer := func(s shard.Scope, inc shard.NamespaceIncarnation, term epoch.Epoch) store.Writer {
		w := b.Writer(s, inc, term)
		return NewWriterStub(h, WriterStubDelegateTo(w), WithWriterCommit(
			func(ctx context.Context, expect epoch.Epoch, m *store.Mutation) (epoch.Epoch, error) {
				if _, err := m.Digest(); err != nil {
					encodable := *m
					encodable.Transitions = nil
					return w.Commit(ctx, expect, &encodable)
				}
				return w.Commit(ctx, expect, m)
			}))
	}
	return plant(h, b, WithStoreWriter(writer))
}

// LostRequestsApplyOnTheirRetry checks that a commit whose request a fault loses has no
// receipt until its retry applies it once.
//
//dokimi:check Store.Writer defect=appliesLostRequests
func LostRequestsApplyOnTheirRetry(h *StoreHandle) {
	settles(h, LoseRequest, false)
}

// appliesLostRequests returns a planted defect whose lost request applies its commit, as a
// lost reply does.
func appliesLostRequests(h *StoreHandle) store.Store {
	s := plant(h, newBackend(h))
	s.points = map[Loss]memory.Point{LoseRequest: memory.PointCommitAck, LoseReply: memory.PointCommitAck}
	return s
}

// LostRepliesKeepTheirCommit checks that a commit whose reply a fault loses keeps its
// receipt and returns its generation to its retry.
//
//dokimi:check Store.Writer defect=dropsCommitsOfLostReplies
func LostRepliesKeepTheirCommit(h *StoreHandle) {
	settles(h, LoseReply, true)
}

// dropsCommitsOfLostReplies returns a planted defect whose lost reply drops its commit, as
// a lost request does.
func dropsCommitsOfLostReplies(h *StoreHandle) store.Store {
	s := plant(h, newBackend(h))
	s.points = map[Loss]memory.Point{LoseRequest: memory.PointCommitApply, LoseReply: memory.PointCommitApply}
	return s
}

// ReceiptsEndWithTheirIncarnation checks that Outcome returns no receipt of an incarnation
// whose scope another incarnation created again.
//
//dokimi:check Store.Outcome defect=answersDeletedIncarnations
func ReceiptsEndWithTheirIncarnation(h *StoreHandle) {
	if _, ok := recreate(h); !ok {
		return
	}
	_, ok, err := h.Subject().Outcome(h.Context(), scope, store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 2})
	assert.NoError(h, err, "Outcome returns the receipt of a commit")
	assert.False(h, ok, h.Statement())
}

// answersDeletedIncarnations returns a planted defect whose Outcome of a commit ID of a
// deleted scope returns the receipt of the same term and number in the incarnation that
// created the scope again.
func answersDeletedIncarnations(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreOutcome(func(ctx context.Context, sc shard.Scope, cid store.CommitID) (
		store.Receipt, bool, error,
	) {
		r, ok, err := b.Outcome(ctx, sc, cid)
		if err != nil || ok || !b.wasDeleted(sc) {
			return r, ok, err
		}
		head, err := b.Head(ctx, sc)
		if err != nil {
			return store.Receipt{}, false, err
		}
		return b.Outcome(ctx, sc, store.CommitID{Incarnation: head.Incarnation, Term: cid.Term, Seq: cid.Seq})
	}))
}

// TermsAreExclusive checks that Campaign grants each term to one candidate at a time, above
// the terms before it.
//
// Concurrent candidates each lead their terms and resign. Each candidate campaigns once
// with an ended context, and only such a campaign returns an error.
//
//dokimi:check Store.Campaign defect=reusesTerms
func TermsAreExclusive(h *StoreHandle) {
	s := h.Subject()
	live := h.Context()
	ended, cancel := context.WithCancel(live)
	cancel()
	calls := history.New()
	outcomes := history.Concurrently(candidates, waitFor, func(client int) (any, error) {
		for round := range rounds {
			ctx := live
			if (client+round)%candidates == 0 {
				ctx = ended
			}
			call := calls.Invoke(client, opLead, nil)
			l, err := s.Campaign(ctx, scope)
			if err != nil {
				expect.Equal(h, ctx, ended, "Campaign returns an error only for an ended context", assert.ByIdentity())
				call.Fail(err)
				continue
			}
			call.OK(l.Term())
			call = calls.Invoke(client, opResign, nil)
			expect.NoError(h, l.Resign(live), "Resign returns nil")
			call.OK(nil)
		}
		return client, nil
	})
	for _, o := range outcomes {
		assert.True(h, o.Finished, "each candidate leads its terms within the wait of the suite")
	}
	history.Linearizable(h, calls, termSpec, h.Statement())
}

// reusesTerms returns a planted defect whose every term is the term after the watermark of
// its scope.
func reusesTerms(h *StoreHandle) store.Store {
	return termDefect(h, func(_, watermark epoch.Epoch) epoch.Epoch { return watermark + 1 })
}

// TermsExceedTheWatermark checks that Campaign grants a term above the watermark of the
// scope.
//
//dokimi:check Store.Campaign defect=countsTermsFromTheWatermark
func TermsExceedTheWatermark(h *StoreHandle) {
	s := h.Subject()
	m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: highTerm, Seq: 1}}
	_, err := s.Writer(scope, incA, highTerm).Commit(h.Context(), 0, &m)
	assert.NoError(h, err, "Commit applies the commit that raises the watermark")
	l, err := s.Campaign(h.Context(), scope)
	assert.NoError(h, err, "Campaign grants a term")
	assert.True(h, l.Term() > highTerm, h.Statement())
	assert.NoError(h, l.Resign(h.Context()), "Resign returns nil")
}

// countsTermsFromTheWatermark returns a planted defect whose every term counts the terms
// after the watermark of its scope.
func countsTermsFromTheWatermark(h *StoreHandle) store.Store {
	return termDefect(h, func(term, watermark epoch.Epoch) epoch.Epoch { return term - watermark })
}

// CampaignsHonourCancellation checks that Campaign returns the error of its context while
// another candidate leads.
//
//dokimi:check Store.Campaign defect=hidesCampaignCancellations
func CampaignsHonourCancellation(h *StoreHandle) {
	s := h.Subject()
	l, err := s.Campaign(h.Context(), scope)
	assert.NoError(h, err, "Campaign grants a term")
	assert.HonoursCancellation(h, func(ctx context.Context) error {
		_, err := s.Campaign(ctx, scope)
		// The wrap of a nil error matches no cancellation, so a Campaign that returns nil
		// fails the check.
		return fmt.Errorf("storetest: campaign while another candidate leads: %w", err)
	}, h.Statement())
	assert.NoError(h, l.Resign(h.Context()), "Resign returns nil")
}

// hidesCampaignCancellations returns a planted defect whose Campaign returns errStopped in
// place of the error of its ended context.
func hidesCampaignCancellations(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreCampaign(func(ctx context.Context, s shard.Scope) (store.Leadership, error) {
		l, err := b.Campaign(ctx, s)
		if err != nil && ctx.Err() != nil {
			return nil, errStopped
		}
		return l, err
	}))
}

// TermsEndWithTheirCampaign checks that a term ends once the context of the campaign that
// granted it ends.
//
//dokimi:check Store.Campaign defect=outlivesCampaigns
func TermsEndWithTheirCampaign(h *StoreHandle) {
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	l, err := h.Subject().Campaign(ctx, scope)
	assert.NoError(h, err, "Campaign grants a term")
	cancel()
	streamtest.Receive(h, l.Done(), waitFor, h.Statement())
}

// outlivesCampaigns returns a planted defect whose terms report their end only after Resign
// or Close.
func outlivesCampaigns(h *StoreHandle) store.Store {
	b := newBackend(h)
	closed := make(chan struct{})
	var once sync.Once
	end := func() { once.Do(func() { close(closed) }) }
	h.Cleanup(end)
	return plant(h, b,
		WithStoreClose(func() error {
			end()
			return b.Close()
		}),
		WithStoreCampaign(func(ctx context.Context, s shard.Scope) (store.Leadership, error) {
			l, err := b.Campaign(ctx, s)
			if err != nil {
				return l, err
			}
			done := make(chan struct{})
			var resign sync.Once
			stop := func() { resign.Do(func() { close(done) }) }
			go func() {
				<-closed
				stop()
			}()
			return NewLeadershipStub(h, LeadershipStubDelegateTo(l),
				WithLeadershipDone(func() <-chan struct{} { return done }),
				WithLeadershipResign(func(ctx context.Context) error {
					defer stop()
					return l.Resign(ctx)
				})), nil
		}))
}

// ResignationsElectTheNextCandidate checks that a waiting candidate leads once the leader
// resigns.
//
//dokimi:check Store.Campaign defect=keepsTermsAfterResign
func ResignationsElectTheNextCandidate(h *StoreHandle) {
	s := h.Subject()
	first, err := s.Campaign(h.Context(), scope)
	assert.NoError(h, err, "Campaign grants a term")
	type grant struct {
		l   store.Leadership
		err error
	}
	grants := make(chan grant, 1)
	go func() {
		l, err := s.Campaign(h.Context(), scope)
		grants <- grant{l: l, err: err}
	}()
	assert.NoError(h, first.Resign(h.Context()), "Resign returns nil")
	streamtest.Receive(h, first.Done(), waitFor, h.Statement())
	next, _ := streamtest.Receive(h, grants, waitFor, h.Statement())
	assert.NoError(h, next.err, h.Statement())
	assert.NoError(h, next.l.Resign(h.Context()), "Resign returns nil")
}

// keepsTermsAfterResign returns a planted defect whose terms report their end after the end
// of their campaign or after Close, and not after Resign.
func keepsTermsAfterResign(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreCampaign(func(ctx context.Context, s shard.Scope) (store.Leadership, error) {
		l, err := b.Campaign(ctx, s)
		if err != nil {
			return l, err
		}
		done := make(chan struct{})
		var resigned atomic.Bool
		go func() {
			<-l.Done()
			if !resigned.Load() {
				close(done)
			}
		}()
		return NewLeadershipStub(h, LeadershipStubDelegateTo(l),
			WithLeadershipDone(func() <-chan struct{} { return done }),
			WithLeadershipResign(func(ctx context.Context) error {
				resigned.Store(true)
				return l.Resign(ctx)
			})), nil
	}))
}

// FitsMatchesCommit checks that Fits returns ErrTooLarge for each mutation that Commit
// refuses as too large, and nil for each mutation that Commit applies.
//
//dokimi:check Store.Fits defect=fitsEverything
func FitsMatchesCommit(h *StoreHandle, c *prop.Case) {
	s := h.Subject()
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
			Shard: shard.ID{ID128: id.ID128{0: tagFits, 14: byte(i >> 8), 15: byte(i)}},
			Range: shard.Range{Start: []byte{byte(i >> 8), byte(i)}},
		}
	}
	fits := s.Fits(scope, &m)
	_, committed := s.Writer(scope, incA, lawTerm).Commit(h.Context(), 0, &m)
	if fits == nil {
		assert.NoError(h, committed, h.Statement())
		return
	}
	assert.ErrorIs(h, fits, store.ErrTooLarge, h.Statement())
	assert.ErrorIs(h, committed, store.ErrTooLarge, h.Statement())
	got, err := s.Head(h.Context(), scope)
	assert.NoError(h, err, "Head returns the header of the scope")
	assert.Equal(h, got, store.Head{}, h.Statement())
}

// fitsEverything returns a planted defect whose Fits returns nil for every mutation.
func fitsEverything(h *StoreHandle) store.Store {
	return plant(h, newBackend(h), WithStoreFits(func(shard.Scope, *store.Mutation) error { return nil }))
}

// CloseEndsWatchesAndTerms checks that Close ends the watches and the terms of the store.
//
//dokimi:check Store.Close defect=keepsWatchesAfterClose
func CloseEndsWatchesAndTerms(h *StoreHandle) {
	s := h.Subject()
	cm := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	cm.commit(h, &store.Mutation{})
	l, err := s.Campaign(h.Context(), scope)
	assert.NoError(h, err, "Campaign grants a term")
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, s.Watch(ctx, scope, incA, 0, ""))
	_, ok := streamtest.Receive(h, items, waitFor, "Watch yields the generation of the scope within the wait")
	assert.True(h, ok, "Watch yields the generation of the scope")
	assert.NoError(h, s.Close(), "Close returns nil")
	streamtest.Receive(h, l.Done(), waitFor, h.Statement())
	streamtest.End(h, items, waitFor, h.Statement())
}

// keepsWatchesAfterClose returns a planted defect whose watches wait for the end of their
// context after Close.
func keepsWatchesAfterClose(h *StoreHandle) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreWatch(func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation,
		from epoch.Epoch, node shard.NodeID,
	) iter.Seq2[store.Event, error] {
		events := b.Watch(ctx, s, inc, from, node)
		return func(yield func(store.Event, error) bool) {
			for e, err := range events {
				if errors.Is(err, memory.ErrClosed) {
					<-ctx.Done()
					return
				}
				if !yield(e, err) {
					return
				}
			}
		}
	}))
}

// restartsPages returns a planted defect whose Read of a page token returns the first page
// of the scope in place of an error that wraps target.
func restartsPages(h *StoreHandle, target error) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreRead(
		func(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation, q store.Query, p page.Page) (
			store.State, page.Cursor[shard.Placement], error,
		) {
			state, cur, err := b.Read(ctx, s, inc, q, p)
			if errors.Is(err, target) && p.Token != "" {
				return b.Read(ctx, s, inc, q, page.Page{Limit: p.Limit})
			}
			return state, cur, err
		}))
}

// termDefect returns a planted defect whose every term is the term that term returns for
// the term that the memory store granted and the watermark of the scope at the grant.
func termDefect(h *StoreHandle, term func(granted, watermark epoch.Epoch) epoch.Epoch) store.Store {
	b := newBackend(h)
	return plant(h, b, WithStoreCampaign(func(ctx context.Context, s shard.Scope) (store.Leadership, error) {
		l, err := b.Campaign(ctx, s)
		if err != nil {
			return l, err
		}
		head, err := b.Head(ctx, s)
		if err != nil {
			return nil, errors.Join(err, l.Resign(ctx))
		}
		changed := term(l.Term(), head.Watermark)
		return NewLeadershipStub(h, LeadershipStubDelegateTo(l),
			WithLeadershipTerm(func() epoch.Epoch { return changed })), nil
	}))
}

// operation returns the subject of h as the operation O of the kit. It reports false, and
// ends the check with a fault that wraps ErrOperation, when the subject does not implement
// O.
func operation[O any](h *StoreHandle) (O, bool) {
	o, ok := h.Subject().(O)
	if !ok {
		assert.Fault(h, fmt.Errorf("%w: %T does not implement %v", ErrOperation, h.Subject(), reflect.TypeFor[O]()))
	}
	return o, ok
}

// recreate commits two generations to the scope of the subject of h through a writer of
// the incarnation A, and reads the first page of the scope with a limit of one placement.
// It then deletes the scope and commits two generations through a writer of the
// incarnation B. It returns the token of the page. It reports false, and ends the check
// with a fault, when the subject does not implement Deleter.
func recreate(h *StoreHandle) (string, bool) {
	deleter, ok := operation[Deleter](h)
	if !ok {
		return "", false
	}
	s := h.Subject()
	a := &committer{w: s.Writer(scope, incA, lawTerm), inc: incA}
	a.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere[:2])})
	a.commit(h, &store.Mutation{Spec: []byte(`{}`)})
	_, cur, err := s.Read(h.Context(), scope, incA, store.Query{}, page.Page{Limit: 1})
	assert.NoError(h, err, "Read returns the first page of the scope")
	_, token := drain(h, cur, 1)
	assert.NotEqual(h, token, "", "Read returns a token for a scope of two placements and a limit of one")
	deleter.Delete(scope)
	b := &committer{w: s.Writer(scope, incB, lawTerm), inc: incB}
	b.commit(h, &store.Mutation{Put: slices.Clone(fx.everywhere[:2])})
	b.commit(h, &store.Mutation{Spec: []byte(`{}`)})
	return token, true
}

// commitsToTheRoot commits the mutations that c draws to the root scope of the namespace,
// through a writer of the incarnation A, when c draws commits there.
func commitsToTheRoot(h *StoreHandle, c *prop.Case, s store.Store) {
	if !c.Draw(prop.Boolean(), "commits to the root scope") {
		return
	}
	cm := &committer{w: s.Writer(root, incA, lawTerm), inc: incA}
	ms := c.Draw(fx.mutations, "mutations of the root scope")
	for i := range ms {
		cm.commit(h, &ms[i])
	}
}

// settles checks that Outcome reports whether a commit that the fault l interrupted
// applied, and that a retry of the commit applies it at most once.
func settles(h *StoreHandle, l Loss, applied bool) {
	faulty, ok := operation[Faulty](h)
	if !ok {
		return
	}
	s := h.Subject()
	w := s.Writer(scope, incA, lawTerm)
	m := store.Mutation{ID: store.CommitID{Incarnation: incA, Term: lawTerm, Seq: 1}, Spec: []byte(`{}`)}
	d, err := m.Digest()
	assert.NoError(h, err, "the mutation of the check encodes")
	faulty.LoseCommit(l)
	_, err = w.Commit(h.Context(), 0, &m)
	assert.ErrorIs(h, err, version.ErrOutcomeUnknown, "a commit that a fault interrupts returns ErrOutcomeUnknown")
	_, ok, err = s.Outcome(h.Context(), scope, m.ID)
	assert.NoError(h, err, "Outcome returns the receipt of a commit")
	assert.Equal(h, ok, applied, h.Statement())
	g, err := w.Commit(h.Context(), 0, &m)
	assert.NoError(h, err, h.Statement())
	assert.Equal(h, g, epoch.Epoch(1), h.Statement())
	r, ok, err := s.Outcome(h.Context(), scope, m.ID)
	assert.NoError(h, err, "Outcome returns the receipt of a commit")
	assert.True(h, ok, h.Statement())
	assert.Equal(h, r, store.Receipt{ID: m.ID, Digest: d, Generation: 1}, h.Statement())
}

// generateCommit generates the input of a commit of the machine of commits. The input has a
// writer, a number, a variant and the generation that the commit expects.
func generateCommit(c *prop.Case, _ struct{}) any {
	return commitCall{
		w:       c.Draw(prop.Integer(0, len(writers)-1), "writer"),
		seq:     c.Draw(prop.Integer(1, seqs), "number"),
		variant: c.Draw(prop.Integer(0, variants-1), "variant"),
		expect:  c.Draw(prop.Integer[epoch.Epoch](0, expects), "expect"),
	}
}

// generateOutcome generates the input of a call of Outcome of the machine of commits. The
// input has a writer and a number.
func generateOutcome(c *prop.Case, _ struct{}) any {
	return outcomeCall{
		w:   c.Draw(prop.Integer(0, len(writers)-1), "writer"),
		seq: c.Draw(prop.Integer(1, seqs), "number"),
	}
}

// generateLoss generates the point at which the next commit of the machine of commits fails.
func generateLoss(c *prop.Case, _ struct{}) any {
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
// placements on one page, and the receipts of the commits of cm. The image has the zero
// Receipt for each commit whose receipt s does not have.
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
		if !ok {
			r = store.Receipt{}
		}
		im.receipts = append(im.receipts, r)
	}
	return im
}

// sameChange fails h at its statement when the event got of a watch of a node differs from
// want: another generation, commit ID or controller record, other placements,
// transitions, ended shards or removed shards in any order, or another change of the
// mutation.
func sameChange(h *StoreHandle, got, want *store.Event) {
	h.Helper()
	assert.Equal(h, got.Generation, want.Generation, h.Statement())
	assert.Permutation(h, got.Mutation.Put, want.Mutation.Put, h.Statement(), assert.EquateEmpty())
	assert.Permutation(h, got.Mutation.Transitions, want.Mutation.Transitions, h.Statement(), assert.EquateEmpty())
	assert.Permutation(h, got.Mutation.Done, want.Mutation.Done, h.Statement(), assert.EquateEmpty())
	assert.Permutation(h, got.Removed, want.Removed, h.Statement(), assert.EquateEmpty())
	rest := got.Mutation
	rest.Put, rest.Transitions, rest.Done = nil, nil, nil
	assert.Equal(h, rest, store.Mutation{ID: want.Mutation.ID, Cell: want.Mutation.Cell}, h.Statement(),
		assert.EquateEmpty())
}

// endsWith checks that a watch of the subject of h from the generation from, for the
// incarnation inc, yields an error that wraps target and then ends. It fails h at its
// statement otherwise.
func endsWith(h *StoreHandle, inc shard.NamespaceIncarnation, from epoch.Epoch, target error) {
	h.Helper()
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	items := streamtest.Follow(ctx, h.Subject().Watch(ctx, scope, inc, from, ""))
	it, ok := streamtest.Receive(h, items, waitFor, h.Statement())
	assert.True(h, ok, h.Statement())
	assert.ErrorIs(h, it.Err, target, h.Statement())
	streamtest.End(h, items, waitFor, h.Statement())
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
