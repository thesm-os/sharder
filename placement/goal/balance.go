// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"fmt"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// BalanceName is the registered name of the goal balance.
const BalanceName shard.StrategyName = "balance"

// BalanceConfig is the configuration of the goal balance.
//
// The zero BalanceConfig is not valid, because it has no metric.
type BalanceConfig struct {
	// Metric is the name of the metric whose utilisation the goal balances, such as
	// "cpu".
	Metric string `json:"metric"`
}

// Validate returns nil when c is valid.
//
// Error modes:
//   - an error that wraps ErrConfig when Metric is empty.
func (c BalanceConfig) Validate() error {
	if c.Metric == "" {
		return fmt.Errorf("%w: metric is empty", ErrConfig)
	}
	return nil
}

// Balance is the goal balance. Its cost is the variance of the utilisation of one
// metric over the accepting nodes with a positive capacity for the metric.
// [NewBalance] returns a Balance, and the zero Balance is not ready for use.
//
// Delta reads the utilisation of the two nodes of a move and the sum that
// placement.State.UtilisationSum returns, so it does not visit the other nodes.
//
// # Concurrency
//
// A Balance does not change after NewBalance returns it, and it is safe for concurrent
// use.
//
// # Allocation contract
//
// Delta does not allocate.
//
//dokimi:subject placement.Goal new=balanceSubject options=cells
type Balance struct {
	// metric is the name of the metric.
	metric string
}

var _ placement.Goal = (*Balance)(nil)

// NewBalance returns the goal balance with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewBalance(cfg BalanceConfig) (*Balance, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Balance{metric: cfg.Metric}, nil
}

// Name returns BalanceName.
func (*Balance) Name() shard.StrategyName {
	return BalanceName
}

// Cost returns the variance of the utilisation of the metric over the accepting nodes
// with a positive capacity for it, or 0 for a state without such nodes.
func (g *Balance) Cost(st *placement.State) float64 {
	sum, count := 0.0, 0
	for _, n := range st.Nodes() {
		if u, ok := st.Utilisation(n, g.metric); ok {
			sum += u
			count++
		}
	}
	if count == 0 {
		return 0
	}
	mean := sum / float64(count)
	cost := 0.0
	for _, n := range st.Nodes() {
		if u, ok := st.Utilisation(n, g.metric); ok {
			d := u - mean
			cost += d * d
		}
	}
	return cost / float64(count)
}

// Delta returns the change of the variance that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (g *Balance) Delta(st *placement.State, m placement.Move) float64 {
	sum, count := st.UtilisationSum(g.metric)
	if m.From == m.To || count == 0 {
		return 0
	}
	load := st.ShardLoad(m.Shard)[g.metric]
	squares, shift := 0.0, 0.0
	if before, after, ok := utilisation(st, m.From, g.metric, -load); ok {
		squares += after*after - before*before
		shift += after - before
	}
	if before, after, ok := utilisation(st, m.To, g.metric, load); ok {
		squares += after*after - before*before
		shift += after - before
	}
	nodes := float64(count)
	return squares/nodes - shift*(shift+2*sum)/(nodes*nodes)
}

// utilisation returns the utilisation of node n on metric before and after a change
// of its load by load, and reports whether n is accepting with a positive capacity for
// metric.
func utilisation(st *placement.State, n shard.NodeID, metric string, load float64) (before, after float64, ok bool) {
	before, ok = st.Utilisation(n, metric)
	if !ok {
		return 0, 0, false
	}
	capacity, _ := st.Capacity(n, metric)
	return before, before + load/capacity, true
}
