// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"errors"
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// PreferName is the registered name of the goal prefer.
const PreferName shard.StrategyName = "prefer"

// PreferConfig is the configuration of the goal prefer.
//
// The zero PreferConfig is not valid, because it has no labels.
type PreferConfig struct {
	// Label is the key of the label of nodes whose value a shard prefers, such as
	// "region".
	Label string `json:"label"`
	// From is the key of the label of shards whose value is the preferred value of
	// Label, such as "home".
	From string `json:"from"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when Label is empty.
//   - an error that wraps ErrConfig when From is empty.
func (c PreferConfig) Validate() error {
	var problems []error
	if c.Label == "" {
		problems = append(problems, fmt.Errorf("%w: label is empty", ErrConfig))
	}
	if c.From == "" {
		problems = append(problems, fmt.Errorf("%w: from is empty", ErrConfig))
	}
	return errors.Join(problems...)
}

// Prefer is the goal prefer. Its cost counts the replicas outside the value that their
// shard prefers. The value of the label From of a shard is its preferred value of
// Label, and a replica is outside it in these cases:
//
//   - the node of the replica has another value of Label.
//   - the node of the replica does not have Label.
//   - the state does not describe the node of the replica.
//
// A shard without the label From adds 0 to the cost. [NewPrefer] returns a Prefer, and
// the zero Prefer is not ready for use.
//
// # Concurrency
//
// A Prefer does not change after NewPrefer returns it, and it is safe for concurrent
// use.
//
// # Allocation contract
//
// Delta does not allocate.
//
//dokimi:subject placement.Goal new=preferSubject options=cells
type Prefer struct {
	// label is the key of the label of nodes.
	label string
	// from is the key of the label of shards.
	from string
}

var _ placement.Goal = (*Prefer)(nil)

// NewPrefer returns the goal prefer with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewPrefer(cfg PreferConfig) (*Prefer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Prefer{label: cfg.Label, from: cfg.From}, nil
}

// Name returns PreferName.
func (*Prefer) Name() shard.StrategyName {
	return PreferName
}

// Cost returns the number of replicas outside the value that their shard prefers.
func (g *Prefer) Cost(st *placement.State) float64 {
	cost := 0
	for _, sh := range st.Shards() {
		want, ok := g.preferred(st, sh)
		if !ok {
			continue
		}
		for _, r := range st.Replicas(sh) {
			if g.outside(st, r.Node, want) {
				cost++
			}
		}
	}
	return float64(cost)
}

// Delta returns the change of the cost that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (g *Prefer) Delta(st *placement.State, m placement.Move) float64 {
	want, ok := g.preferred(st, m.Shard)
	if !ok {
		return 0
	}
	// For a move whose From equals its To, the two terms cancel.
	delta := 0.0
	if m.From != "" && g.outside(st, m.From, want) {
		delta--
	}
	if m.To != "" && g.outside(st, m.To, want) {
		delta++
	}
	return delta
}

// preferred returns the value of the label From of shard sh, and reports whether the
// shard has the label.
func (g *Prefer) preferred(st *placement.State, sh shard.ID) (string, bool) {
	s, _ := st.Shard(sh)
	label, ok := s.Labels.Find(g.from)
	return label.Value, ok
}

// outside reports whether a replica on node n is outside the preferred value want. A
// node that the state does not describe, or that does not have the label, is outside
// every value.
func (g *Prefer) outside(st *placement.State, n shard.NodeID, want string) bool {
	// Node returns a node without labels for a node that the state does not describe.
	node, _ := st.Node(n)
	label, ok := node.Labels.Find(g.label)
	return !ok || label.Value != want
}
