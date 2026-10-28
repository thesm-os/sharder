// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// CountName is the registered name of the goal count.
const CountName shard.StrategyName = "count"

// CountConfig is the configuration of the goal count.
//
// With the zero CountConfig, every role has the weight 1.
//
// # Encoding
//
// encoding/json encodes RoleWeights as an object whose keys are the spellings of the
// roles, such as {"primary": 2, "secondary": 1}.
//
// # Concurrency
//
// A copy of a CountConfig shares the map of RoleWeights with the original.
type CountConfig struct {
	// RoleWeights maps a role to the weight of each replica in the role. A role
	// without an entry has the weight 1.
	RoleWeights map[shard.Role]float64 `json:"roleWeights,omitempty"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig. It reports the problems in the order of the roles.
//
// Error modes:
//   - an error that wraps ErrConfig for a weight of shard.RoleNone, and for a weight
//     of a role that is not valid.
//   - an error that wraps ErrConfig for a weight that is negative or not finite.
func (c CountConfig) Validate() error {
	var problems []error
	for _, r := range slices.Sorted(maps.Keys(c.RoleWeights)) {
		if r == shard.RoleNone || !r.IsValid() {
			problems = append(problems, fmt.Errorf("%w: a weight of the role %v", ErrConfig, r))
		}
		if w := c.RoleWeights[r]; !(w >= 0) || math.IsInf(w, 1) {
			problems = append(problems, fmt.Errorf("%w: the role %v has the weight %v", ErrConfig, r, w))
		}
	}
	return errors.Join(problems...)
}

// Count is the goal count. Its cost is the variance of the weighted replica counts of
// the accepting nodes. The weighted count of a node is the sum of the weights of the
// roles of its replicas. [NewCount] returns a Count, and every role has the weight 0
// in the zero Count.
//
// A move between two accepting nodes changes two counts, so Delta reads the counts of
// two nodes and the totals of the roles. A move from or to a node that is not
// accepting changes the count of one node and the mean.
//
// # Concurrency
//
// A Count does not change after NewCount returns it, and it is safe for concurrent
// use.
//
// # Allocation contract
//
// Delta does not allocate.
//
//dokimi:subject placement.Goal new=countSubject options=cells
type Count struct {
	// weights is the weight of each role, indexed by the role.
	weights [shard.RoleCount]float64
}

var _ placement.Goal = (*Count)(nil)

// NewCount returns the goal count with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewCount(cfg CountConfig) (*Count, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	g := &Count{}
	for r := range g.weights {
		w, ok := cfg.RoleWeights[shard.Role(r)]
		if !ok {
			w = 1
		}
		g.weights[r] = w
	}
	return g, nil
}

// Name returns CountName.
func (*Count) Name() shard.StrategyName {
	return CountName
}

// Cost returns the variance of the weighted replica counts of the accepting nodes, or
// 0 for a state without accepting nodes.
func (g *Count) Cost(st *placement.State) float64 {
	nodes := st.Nodes()
	if len(nodes) == 0 {
		return 0
	}
	sum := 0.0
	for _, n := range nodes {
		sum += g.count(st, n)
	}
	mean := sum / float64(len(nodes))
	cost := 0.0
	for _, n := range nodes {
		d := g.count(st, n) - mean
		cost += d * d
	}
	return cost / float64(len(nodes))
}

// Delta returns the change of the variance that m makes, without applying m. It
// returns 0 for a role that is not valid.
//
// # Allocation contract
//
// Zero alloc.
func (g *Count) Delta(st *placement.State, m placement.Move) float64 {
	nodes := float64(len(st.Nodes()))
	if m.From == m.To || nodes == 0 || !m.Role.IsValid() {
		return 0
	}
	w := g.weights[m.Role]
	squares, shift := 0.0, 0.0
	if _, ok := st.Node(m.From); ok {
		squares += w * (w - 2*g.count(st, m.From))
		shift -= w
	}
	if _, ok := st.Node(m.To); ok {
		squares += w * (w + 2*g.count(st, m.To))
		shift += w
	}
	sum := 0.0
	for r, weight := range g.weights {
		sum += weight * float64(st.RoleTotal(shard.Role(r)))
	}
	return squares/nodes - shift*(shift+2*sum)/(nodes*nodes)
}

// count returns the weighted replica count of node n.
func (g *Count) count(st *placement.State, n shard.NodeID) float64 {
	c := 0.0
	for r, w := range g.weights {
		c += w * float64(st.RoleCount(n, shard.Role(r)))
	}
	return c
}
