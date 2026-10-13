// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller

import (
	"context"
	"errors"
	"fmt"

	"go.thesmos.sh/core/errs"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// The calls that the errors of the servers name.
const (
	// callCallback is a callback of a root.
	callCallback = "the callback"
	// callReport is a report of an agent.
	callReport = "the report"
	// callProposal is a proposal of an operator.
	callProposal = "the proposal"
	// callDisruption is a disruption.
	callDisruption = "the disruption"
	// callDone is the end of a reservation.
	callDone = "the end of the reservation"
)

// Do runs the callback r of the root of its namespace through the agent of the root. An add
// starts the controller of the cell r.Shard at the term r.Epoch, and returns once the
// acquisition of the scope of the cell has applied. A drop returns once that controller has
// stopped. The agent of the root checks md and r as the agent of a cell checks a callback of
// its controller. Do does not modify or keep r.
//
// Error modes:
//   - an error that wraps the error of Metadata.Check.
//   - an error that wraps ErrScope for a request whose scope is the scope of a cell.
//   - an error that wraps ErrNotServing for a namespace that the process does not run,
//     before Run, and once Run has returned.
//   - an error that wraps the error with which the agent of the root refuses r, such as an
//     error of class errs.Denied for a caller other than the root.
//   - an error that wraps the error of the acquisition of the cell, such as an error that
//     wraps epoch.ErrFenced.
func (c *Controller) Do(ctx context.Context, md protocol.Metadata, r *protocol.Request) error {
	if err := md.Check(); err != nil {
		return fmt.Errorf("controller: the metadata of %s: %w", callCallback, err)
	}
	if r.Scope.Cell != "" {
		return fmt.Errorf("%w: the callback is for the cell %s", ErrScope, r.Scope.Cell)
	}
	n, err := c.lookup(r.Scope.Namespace)
	if err != nil {
		return err
	}
	req := r.CloneKanon()
	_, err = call[struct{}](ctx, n.agent, callCallback, func(tok machine.ReplyToken) machine.Event {
		return machine.Do{Request: req, Metadata: md, Reply: tok}
	})
	return err
}

// Report passes the report r of an agent to the controller of its cell, and returns the ack
// of the controller. Report does not modify or keep r.
//
// Error modes:
//   - an error that wraps the error of Metadata.Check.
//   - an error that wraps ErrNotServing for a cell that the process does not host, for a
//     namespace that it does not run, before Run, and once Run has returned.
//   - an error that wraps the error of the controller, such as an error of class
//     errs.Conflict for a node incarnation that is not live. The root refuses every report.
func (c *Controller) Report(ctx context.Context, md protocol.Metadata, r *protocol.Report) (protocol.Ack, error) {
	if err := md.Check(); err != nil {
		return protocol.Ack{}, fmt.Errorf("controller: the metadata of %s: %w", callReport, err)
	}
	d, err := c.server(r.Scope)
	if err != nil {
		return protocol.Ack{}, err
	}
	m := r.CloneKanon()
	return call[protocol.Ack](ctx, d, callReport, func(tok machine.ReplyToken) machine.Event {
		return machine.Report{Report: m, Metadata: md, Reply: tok}
	})
}

// Submit passes the proposal p to the controller of its scope. The root takes a spec
// proposal, and the controller of a cell takes every other kind. Submit returns the receipt
// of the controller once the proposal has applied. It does not modify or keep p.
//
// Error modes:
//   - an error that wraps the error of Metadata.Check.
//   - an error that wraps ErrNotServing for a cell that the process does not host, for a
//     namespace that it does not run, before Run, and once Run has returned.
//   - an error that wraps the error with which the controller refuses p, such as an error of
//     class errs.Transient from a root that does not lead.
func (c *Controller) Submit(ctx context.Context, md protocol.Metadata, p *proposal.Proposal) (protocol.Receipt, error) {
	if err := md.Check(); err != nil {
		return protocol.Receipt{}, fmt.Errorf("controller: the metadata of %s: %w", callProposal, err)
	}
	d, err := c.server(p.Scope)
	if err != nil {
		return protocol.Receipt{}, err
	}
	m := p.CloneKanon()
	return call[protocol.Receipt](ctx, d, callProposal, func(tok machine.ReplyToken) machine.Event {
		return machine.Submit{Proposal: m, Metadata: md, Reply: tok}
	})
}

// Disrupt passes the disruption d to the controller of its cell, and returns the decision of
// the budget of the cell once the controller has committed it. Disrupt does not modify or
// keep d.
//
// Error modes:
//   - an error that wraps the error of Metadata.Check.
//   - an error that wraps ErrNotServing for a cell that the process does not host, for a
//     namespace that it does not run, before Run, and once Run has returned.
//   - an error that wraps the error with which the controller refuses d. The root refuses
//     every disruption.
func (c *Controller) Disrupt(ctx context.Context, md protocol.Metadata, d *budget.Disruption) (budget.Decision, error) {
	if err := md.Check(); err != nil {
		return budget.Decision{}, fmt.Errorf("controller: the metadata of %s: %w", callDisruption, err)
	}
	s, err := c.server(d.Scope)
	if err != nil {
		return budget.Decision{}, err
	}
	m := d.CloneKanon()
	return call[budget.Decision](ctx, s, callDisruption, func(tok machine.ReplyToken) machine.Event {
		return machine.Disrupt{Disruption: m, Metadata: md, Reply: tok}
	})
}

// Done passes the end of the reservation a.ID to the controller of its cell, and returns
// once the controller has committed the end. Done does not modify or keep a.
//
// Error modes:
//   - an error that wraps the error of Metadata.Check.
//   - an error that wraps ErrNotServing for a cell that the process does not host, for a
//     namespace that it does not run, before Run, and once Run has returned.
//   - an error that wraps the error with which the controller refuses the end. The root
//     refuses every end of a reservation.
func (c *Controller) Done(ctx context.Context, md protocol.Metadata, a *protocol.DoneArgs) error {
	if err := md.Check(); err != nil {
		return fmt.Errorf("controller: the metadata of %s: %w", callDone, err)
	}
	d, err := c.server(a.Scope)
	if err != nil {
		return err
	}
	m := *a
	_, err = call[struct{}](ctx, d, callDone, func(tok machine.ReplyToken) machine.Event {
		return machine.Done{Args: &m, Metadata: md, Reply: tok}
	})
	return err
}

// server returns the driver of the machine that serves the calls of the scope s: the root
// for the root scope, and the controller of the cell for the scope of a cell.
//
// Error modes:
//   - an error that wraps ErrNotServing for a namespace that the process does not run, and
//     for a cell that it does not host.
func (c *Controller) server(s shard.Scope) (*driver.Driver, error) {
	n, err := c.lookup(s.Namespace)
	if err != nil {
		return nil, err
	}
	if s.Cell == "" {
		return n.root, nil
	}
	return n.host.served(s.Cell)
}

// lookup returns what the process runs for the namespace ns.
//
// Error modes:
//   - an error that wraps ErrNotServing for a namespace that the process does not run, and
//     before Run.
func (c *Controller) lookup(ns shard.Namespace) (*namespace, error) {
	n, ok := (*c.served.Load())[ns]
	if !ok {
		return nil, fmt.Errorf("%w: the namespace %s", ErrNotServing, ns)
	}
	return n, nil
}

// call passes the inbound call that event returns to the machine of d, and returns the
// reply of the machine. The errors of call contain the text what.
//
// Error modes:
//   - an error that wraps ErrNotServing and driver.ErrStopped once the run of d has stopped.
//     It classifies as errs.Transient, the class of ErrNotServing.
//   - an error that wraps the error of the reply, and the cause of ctx when ctx ends first.
func call[T machine.Result](ctx context.Context, d *driver.Driver, what string,
	event func(machine.ReplyToken) machine.Event,
) (T, error) {
	v, err := d.Call[T](ctx, event)
	if errors.Is(err, driver.ErrStopped) {
		return v, errs.WithClass(fmt.Errorf("%w: %s: %w", ErrNotServing, what, err), errs.Transient)
	}
	if err != nil {
		return v, fmt.Errorf("controller: %s: %w", what, err)
	}
	return v, nil
}
