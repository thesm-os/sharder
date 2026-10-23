// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package constraint

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// ErrConfig reports a configuration of a constraint that is not valid, such as a
// headroom of capacity outside 0 to 1. It classifies as errs.Invalid.
var ErrConfig = errs.WithClass(errors.New("constraint: invalid configuration"), errs.Invalid)

// ErrUnknownOperator reports text that is not the spelling of an operator.
// ParseOperator and Operator.UnmarshalText return an error that wraps it, so the decode
// of a configuration with such text fails with it. It classifies as errs.Invalid.
var ErrUnknownOperator = errs.WithClass(errors.New("constraint: unknown operator"), errs.Invalid)

// ErrCapacity reports a move that puts a node above its capacity on a metric. It
// classifies as errs.Conflict.
var ErrCapacity = errs.WithClass(errors.New("constraint: the node is above its capacity"), errs.Conflict)

// ErrLabels reports a replica on a node whose labels fail a rule. It classifies as
// errs.Conflict.
var ErrLabels = errs.WithClass(errors.New("constraint: the labels of the node fail a rule"), errs.Conflict)

// ErrIsolation reports a move that puts too many replicas of one shard under one value
// of a label. It classifies as errs.Conflict.
var ErrIsolation = errs.WithClass(
	errors.New("constraint: too many replicas of the shard share a value of the label"), errs.Conflict)

// ErrDrain reports a replica on a draining node. It classifies as errs.Conflict.
var ErrDrain = errs.WithClass(errors.New("constraint: the node drains"), errs.Conflict)

// ErrMaxPerNode reports a move that gives a node too many replicas. It classifies as
// errs.Conflict.
var ErrMaxPerNode = errs.WithClass(errors.New("constraint: the node has too many replicas"), errs.Conflict)

// ErrMatchLabel reports a replica on a node whose value of a label differs from the
// value of its shard, and a replica of a shard without the label. It classifies as
// errs.Conflict.
var ErrMatchLabel = errs.WithClass(
	errors.New("constraint: the node does not have the value of the label of the shard"), errs.Conflict)
