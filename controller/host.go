// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/task"

	"go.thesmos.sh/sharder/internal/driver"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// host runs the controllers of the cells that the root of one namespace places on the
// process. It is the handler of the agent of the root. The agent runs the callbacks of one
// cell one at a time, so two calls of AddShard and DropShard never concern one cell at once.
type host struct {
	// ctx is the context of the run of the process. The controller of a cell runs under it.
	ctx context.Context //nolint:containedctx // a controller of a cell runs on after the callback that starts it.
	// group runs the controller of each cell as a task of the run of the process.
	group *task.Group
	// store bounds the commits of each cell.
	store store.Store
	// cells maps each cell of the process to its controller.
	cells map[shard.CellID]*hosted
	// drivers is the configuration of the driver of each cell, without its machine.
	drivers driver.Config
	// config is the configuration of the controller of each cell, without the cell of its
	// scope, its term, its incarnation and the bound of a commit.
	config cell.Config
	// mu guards cells.
	mu sync.RWMutex
}

var _ driver.Handler = (*host)(nil)

// AddShard starts the controller of the cell r.Shard at the term r.Epoch, and returns once
// the acquisition of the scope of the cell has applied. A controller of the cell at that
// term that still runs serves the add, so a repeated add does not start a second
// controller.
//
// Error modes:
//   - an error that wraps the error with which the controller of the cell replies to
//     Started, such as the error of an acquisition that a newer controller has fenced.
//   - an error that wraps the cause of ctx when ctx ends before the acquisition has applied.
//   - an error that wraps driver.ErrStopped when the run of the process ends first.
func (h *host) AddShard(ctx context.Context, r *protocol.Request) error {
	id := shard.CellID(ulid.Format(r.Shard.ID()))
	d, err := h.start(id, r) //nolint:contextcheck // the context of the run bounds a controller of a cell.
	//dokimi:mutate-skip ror-true: start fails only once the run of the process has ended. ctx derives from the context of that run, so Ready returns the cause of ctx then
	if err == nil {
		err = d.Ready(ctx)
	}
	if err != nil {
		return fmt.Errorf("controller: the controller of the cell %s at the term %d: %w", id, r.Epoch, err)
	}
	return nil
}

// DropShard stops the controller of the cell r.Shard, and returns once the run of the
// controller has returned. The agent of the root drops only a cell whose add it has passed.
//
// Error modes:
//   - an error that wraps the cause of ctx when ctx ends before the run of the controller
//     has returned. A later drop of the cell waits for the same run.
func (h *host) DropShard(ctx context.Context, r *protocol.Request) error {
	id := shard.CellID(ulid.Format(r.Shard.ID()))
	h.mu.RLock()
	c := h.cells[id]
	h.mu.RUnlock()
	c.stop()
	select {
	case <-c.done:
	case <-ctx.Done():
		return fmt.Errorf("controller: the drop of the cell %s at the term %d: %w", id, c.term, context.Cause(ctx))
	}
	h.mu.Lock()
	delete(h.cells, id)
	h.mu.Unlock()
	return nil
}

// start returns the driver of the controller of the cell id at the term r.Epoch. A controller
// of the cell at that term whose run has not returned keeps serving. Otherwise start stops
// the controller of the cell, if one runs, and starts the controller of the term as a task of
// the run of the process. The new controller runs once the run of the stopped controller has
// returned.
//
// Error modes:
//   - the errors of cell.New and driver.New, which New and the agent of the root rule out.
//   - the error of task.Group.Go once the run of the process has ended.
func (h *host) start(id shard.CellID, r *protocol.Request) (*driver.Driver, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	old := h.cells[id]
	if old != nil && old.term == r.Epoch {
		select {
		case <-old.done:
		default:
			return old.driver, nil
		}
	}
	cfg := h.config
	cfg.Scope.Cell, cfg.Term, cfg.Incarnation = id, r.Epoch, r.Operation.Incarnation
	cfg.Fits = fits(h.store, cfg.Scope)
	m, merr := cell.New(&cfg)
	dc := h.drivers
	dc.Machine = m
	d, derr := driver.New(&dc)
	err := errors.Join(merr, derr)
	var gerr error
	//dokimi:mutate-skip ror-true: New and the agent of the root check every field that cell.New and driver.New refuse, so neither fails here
	if err == nil {
		life, stop := context.WithCancel(h.ctx)
		c := &hosted{driver: d, stop: stop, done: make(chan struct{}), term: r.Epoch}
		h.cells[id] = c
		//dokimi:mutate-skip sbr-delete,ror-false: the agent of the root drops the controller of an older term before it adds a newer one, so old has stopped already
		if old != nil {
			//dokimi:mutate-skip sbr-delete: the agent of the root drops the controller of an older term before it adds a newer one, so old has stopped already
			old.stop()
		}
		gerr = h.group.Go(func(ctx context.Context) error { return c.run(ctx, life, id, old) })
	}
	return d, errors.Join(err, gerr)
}

// served returns the driver of the controller of the cell id.
//
// Error modes:
//   - an error that wraps ErrNotServing for a cell that the process does not host.
func (h *host) served(id shard.CellID) (*driver.Driver, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.cells[id]
	if !ok {
		return nil, fmt.Errorf("%w: the cell %s", ErrNotServing, id)
	}
	return c.driver, nil
}

// hosted is the controller of one cell of the process at one term.
type hosted struct {
	// driver runs the cell machine.
	driver *driver.Driver
	// stop ends the run of the driver.
	stop context.CancelFunc
	// done is closed once the run of the driver has returned. A run that the group refuses
	// does not close it. The group refuses a run only once the run of the process has ended,
	// so every wait for done also ends with the run of the process.
	done chan struct{}
	// term is the term of the controller.
	term epoch.Epoch
}

// run runs the controller c of the cell id under life, once the run of the stopped
// controller old has returned. ctx is the context of the run of the process, from which life
// derives. run returns nil in these cases:
//   - The host or the process ends life.
//   - The controller ends before it serves. AddShard returns its error to the root.
//   - A newer controller of the cell fences the controller. The root moves the cell.
//
// run returns the error of any other end, which stops the process.
func (c *hosted) run(ctx, life context.Context, id shard.CellID, old *hosted) error {
	defer close(c.done)
	if old != nil {
		select {
		case <-old.done:
		case <-life.Done():
		}
	}
	err := c.driver.Run(life)
	if life.Err() != nil || c.driver.Ready(ctx) != nil || errors.Is(err, epoch.ErrFenced) {
		return nil
	}
	return fmt.Errorf("controller: the controller of the cell %s at the term %d: %w", id, c.term, err)
}
