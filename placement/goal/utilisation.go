// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package goal

import (
	"errors"
	"fmt"
	"math"

	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/shard"
)

// UtilisationName is the registered name of the goal utilisation.
const UtilisationName shard.StrategyName = "utilisation"

// UtilisationConfig is the configuration of the goal utilisation.
//
// The zero UtilisationConfig is not valid, because it has no metric.
type UtilisationConfig struct {
	// Metric is the name of the metric whose utilisation the goal measures, such as
	// "cpu".
	Metric string `json:"metric"`
	// Above is the utilisation above which a node adds to the cost. It is at least 0.
	Above float64 `json:"above"`
}

// Validate returns an error that joins every problem of c, or nil when c is valid.
// Each problem wraps ErrConfig.
//
// Error modes:
//   - an error that wraps ErrConfig when Metric is empty.
//   - an error that wraps ErrConfig when Above is negative or not finite.
func (c UtilisationConfig) Validate() error {
	var problems []error
	if c.Metric == "" {
		problems = append(problems, fmt.Errorf("%w: metric is empty", ErrConfig))
	}
	if !(c.Above >= 0) || math.IsInf(c.Above, 1) {
		problems = append(problems, fmt.Errorf("%w: above %v is negative or not finite", ErrConfig, c.Above))
	}
	return errors.Join(problems...)
}

// Utilisation is the goal utilisation. Its cost is the sum of the utilisation above a
// threshold of the accepting nodes with a positive capacity for one metric.
// [NewUtilisation] returns a Utilisation, and the zero Utilisation is not ready for
// use.
//
// # Concurrency
//
// A Utilisation does not change after NewUtilisation returns it, and it is safe for
// concurrent use.
//
// # Allocation contract
//
// Delta does not allocate.
//
//dokimi:subject placement.Goal new=utilisationSubject options=cells
type Utilisation struct {
	// metric is the name of the metric.
	metric string
	// above is the utilisation above which a node adds to the cost.
	above float64
}

var _ placement.Goal = (*Utilisation)(nil)

// NewUtilisation returns the goal utilisation with the configuration cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for each problem that cfg.Validate reports.
func NewUtilisation(cfg UtilisationConfig) (*Utilisation, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Utilisation{metric: cfg.Metric, above: cfg.Above}, nil
}

// Name returns UtilisationName.
func (*Utilisation) Name() shard.StrategyName {
	return UtilisationName
}

// Cost returns the sum of the utilisation above the threshold over the accepting nodes
// with a positive capacity for the metric.
func (g *Utilisation) Cost(st *placement.State) float64 {
	cost := 0.0
	for _, n := range st.Nodes() {
		if u, ok := st.Utilisation(n, g.metric); ok {
			cost += max(0, u-g.above)
		}
	}
	return cost
}

// Delta returns the change of the cost that m makes, without applying m.
//
// # Allocation contract
//
// Zero alloc.
func (g *Utilisation) Delta(st *placement.State, m placement.Move) float64 {
	if m.From == m.To {
		return 0
	}
	load := st.ShardLoad(m.Shard)[g.metric]
	delta := 0.0
	if before, after, ok := utilisation(st, m.From, g.metric, -load); ok {
		delta += max(0, after-g.above) - max(0, before-g.above)
	}
	if before, after, ok := utilisation(st, m.To, g.metric, load); ok {
		delta += max(0, after-g.above) - max(0, before-g.above)
	}
	return delta
}
