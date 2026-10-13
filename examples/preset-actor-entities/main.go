// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command preset-actor-entities runs accounts under the preset actor-entities. An account is an
// entity, and a client sends each deposit of an account through router.Do to the node that has
// the tenure of the shard of the account.
//
// The program runs two application processes, node-a and node-b, in one OS process, over one
// in-memory store and one in-process network. Each process is an embedded.Process, which runs
// the controller of the process, the agent of its node and a router of the namespace. A
// replica.Runner opens each tenure of the node through the bank of the process. The bank loads
// the deposits of the accounts of the shard into memory, and keeps this activation of the
// accounts until the drop of the tenure. The server of the process serves the deposits of the
// active accounts. The client deposits into every account three times:
//
//   - node-a serves the first deposit of every account, because it has every shard.
//   - The second deposit of every account takes its route before node-b starts. The controller
//     then moves half of the shards to node-b. node-a refuses the deposit of each moved account
//     with a NotOwnerError, and router.Do routes the deposit again to node-b.
//   - The third deposit of every account takes its route after the move.
//
// The program then writes one line per account with its deposits, and a last line with the
// number of the deposits that router.Do routed again:
//
//	account-04: 1:node-a 2:node-b 3:node-b
//	redirects: 5
//
// Run it from the examples module:
//
//	go run ./preset-actor-entities
//
// The program logs the errors of the processes at the level error.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/crypto"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/router"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// The namespace of the example.
const (
	// namespace is the namespace of the accounts.
	namespace shard.Namespace = "accounts"
	// slots is the number of the shards of the namespace.
	slots = 8
	// slotsConfig is the configuration of fixed-slots in the spec of the namespace: 8 shards.
	slotsConfig = `{"slots":8}`
	// churnConfig lets one pass of the planner move half of the shards of the cell and 4 shards
	// out of one node.
	churnConfig = `{"churn":{"maxShardFraction":0.5,"perNode":4}}`
)

// The processes of the example.
const (
	// nodeA is the process that starts first. It has every shard until node-b starts.
	nodeA shard.NodeID = "node-a"
	// nodeB is the process that starts during the second round of deposits.
	nodeB shard.NodeID = "node-b"
)

// port is the port of the address of each process.
const port = ":7000"

// The deposits of the example.
const (
	// rounds is the number of the deposits of each account.
	rounds = 3
	// attempts bounds the calls of router.Do for one deposit.
	attempts = 3
	// tick is the time between two reads of the placements of the cell.
	tick = 50 * time.Millisecond
)

// The output and the failure of the program.
const (
	// lineFormat formats the line of one account: its name and its deposits.
	lineFormat = "%s: %s\n"
	// redirectsFormat formats the last line: the number of the deposits that router.Do routed
	// again.
	redirectsFormat = "redirects: %d\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "preset-actor-entities: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// The formats of the entries of the storage of the banks.
const (
	// depositFormat formats the entry of one deposit in the deposits of its account: the number
	// of the deposit and the node that served it.
	depositFormat = "%d:%s"
	// activationFormat formats the entry of one tenure in the activations of its shard: the
	// node and the epoch of the tenure.
	activationFormat = "%s@%d"
)

// names lists the names of the accounts. The name of an account is its key.
var names = []string{
	"account-01", "account-02", "account-03", "account-04", "account-05", "account-06", "account-07", "account-08",
	"account-09", "account-10", "account-11", "account-12", "account-13", "account-14", "account-15", "account-16",
}

// spec is the initial spec of the namespace. It selects the preset actor-entities with 8 slots,
// and lets the planner move up to half of the shards in one pass.
var spec = config.Spec{
	Namespace:    namespace,
	Preset:       builtin.ActorEntities,
	Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	Placement:    config.Selection{Config: json.RawMessage(churnConfig)},
}

// errNoCell is the error of a namespace whose root scope does not list one cell.
var errNoCell = errors.New("main: the namespace does not have one cell")

// prefix is the prefix of a key of the storage of the banks.
type prefix string

// The prefixes of the keys of the storage of the banks.
const (
	// depositsPrefix starts the key of the deposits of an account, which the name of the
	// account ends. The deposits list the deposits of the account in order, separated by
	// spaces.
	depositsPrefix prefix = "deposits/"
	// activationsPrefix starts the key of the activations of a shard, which the ULID of the
	// shard ends. The activations list the tenures of the shard in order, separated by spaces.
	activationsPrefix prefix = "activations/"
)

// activation is the state of the accounts of one shard during a tenure of the node of a bank.
// It contains the deposits of the accounts and the token of the tenure.
type activation struct {
	// deposits maps the name of each account of the shard to its deposits, in order.
	deposits map[string][]string
	// token is the fencing token of the tenure.
	token fence.Token
}

// bankConfig is the configuration of a bank.
type bankConfig struct {
	// storage is the storage of the deposits. The agent of the node acquires its fence for each
	// tenure.
	storage *fencedstore.Store
	// partitioner maps the name of each account to its position.
	partitioner partition.Partitioner
	// node is the node of the bank.
	node shard.NodeID
}

// bank opens the tenures of the shards of its node for a replica.Runner, and activates the
// accounts of each tenure. An account belongs to the shard whose range contains the position
// of its name.
//
// Open loads the deposits of the accounts of the shard from a state that contains the
// acquisition of the fence, and keeps them in memory as the activation of the shard. The work
// of the tenure ends the activation when its context ends, which happens at the drop of the
// tenure. A server serves the deposits of an active account from the activation, and writes
// each deposit through the fence of the tenure. The fence refuses the writes of an earlier
// tenure from the acquisition on, so a deposit never changes an account after a later tenure
// has loaded it.
//
// # Concurrency
//
// A bank is safe for concurrent use. It serves one deposit at a time, and ends an activation
// only between two deposits.
type bank struct {
	// replica is the replica of the storage that Open reads when the replica has applied the
	// acquisition of the tenure.
	replica *fencedstore.Replica
	// active maps each shard with an activation on the node to the activation.
	active map[shard.ID]activation
	// cfg is the configuration of the bank.
	cfg bankConfig
	// mu guards active, and orders the deposits and the end of each activation.
	mu sync.Mutex
}

var _ replica.Opener = (*bank)(nil)

// newBank returns the bank of cfg without an activation, with a new replica of the storage of
// cfg.
func newBank(cfg *bankConfig) *bank {
	return &bank{replica: cfg.storage.NewReplica(), active: make(map[shard.ID]activation), cfg: *cfg}
}

// Open appends the tenure of req to the activations of its shard, in one transaction with the
// key of the add, so an add that the bank receives again does not append the tenure twice. It
// then loads the deposits of each account of the shard through Store.Reader, and activates the
// accounts. The work of the tenure ends the activation once its context ends, and returns nil.
//
// Error modes:
//   - an error that wraps the error of the transaction, such as epoch.ErrFenced for a tenure
//     that a later tenure has fenced, and the cause of ctx.
func (b *bank) Open(ctx context.Context, req *replica.OpenRequest) (replica.OpenResult, error) {
	r, t := req.Request, req.Token
	sid := r.Shard
	key := r.Key()
	k := string(activationsPrefix) + ulid.Format(sid.ID())
	entry := fmt.Sprintf(activationFormat, b.cfg.node, t.Epoch)
	writer := b.cfg.storage.Writer(t)
	err := b.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
		if tx.Recorded(&key) {
			return nil
		}
		if err := writer.Check(ctx, tx); err != nil {
			//nolint:wrapcheck // Open wraps the error of the transaction.
			return err
		}
		list, _ := tx.Get(k)
		tx.Put(k, strings.TrimSpace(list+" "+entry))
		tx.Record(&key)
		return nil
	})
	if err != nil {
		return replica.OpenResult{}, fmt.Errorf(errorPrefix+"the tenure of shard %v at epoch %d: %w", sid, t.Epoch, err)
	}
	src := b.cfg.storage.Reader(&t, b.replica)
	deposits := make(map[string][]string)
	for _, name := range b.accountsOf(r.Range) {
		deposits[name] = depositsOf(src, name)
	}
	b.mu.Lock()
	b.active[sid] = activation{deposits: deposits, token: t}
	b.mu.Unlock()
	return replica.OpenResult{Run: func(ctx context.Context) error {
		<-ctx.Done()
		b.mu.Lock()
		delete(b.active, sid)
		b.mu.Unlock()
		return nil
	}}, nil
}

// accountsOf returns the names of the accounts whose positions r contains, in the order of
// names.
func (b *bank) accountsOf(r shard.Range) []string {
	var found []string
	for _, name := range names {
		if r.Contains(b.cfg.partitioner.Position(nil, []byte(name))) {
			found = append(found, name)
		}
	}
	return found
}

// depositRequest is the request of server.Deposit.
type depositRequest struct {
	// account is the name of the account, which is its key.
	account string
	// epoch is the epoch of the route that the client took for the account.
	epoch epoch.Epoch
}

// depositResult is the result of server.Deposit.
type depositResult struct {
	// deposit is the number of the deposit in the deposits of its account.
	deposit int
}

// server serves the deposits of the accounts that the bank of its node has activated. It
// serves a deposit only while the bank has the activation of the shard of the account at the
// epoch of the route of the client. It refuses every other deposit with a *router.NotOwnerError
// before it starts the deposit, so router.Do routes the account again.
//
// # Concurrency
//
// A server is safe for concurrent use.
type server struct {
	// bank is the bank of the node.
	bank *bank
	// router is the router of the process. It returns the shard of each account.
	router router.Router
}

// Deposit appends one deposit to the deposits of req.account in the activation of its shard,
// and writes the deposits of the account in a transaction that checks the fence with the
// token of the tenure. It returns the number of the deposit.
//
// Error modes:
//   - an error that wraps the error of Router.Route, such as router.ErrNoRoute.
//   - a *router.NotOwnerError when the bank does not have the activation of the shard of the
//     account at req.epoch. The error has the cell, the generation and the namespace
//     incarnation that the router of the server has observed, and the writer of its route as
//     the hint.
//   - an error that wraps the error of the transaction, such as epoch.ErrFenced once a later
//     tenure has acquired the fence, and the cause of ctx. The activation then keeps the
//     deposits that it had.
func (s *server) Deposit(ctx context.Context, req *depositRequest) (depositResult, error) {
	res, err := s.router.Route(ctx, &router.RouteRequest{Key: []byte(req.account)})
	if err != nil {
		return depositResult{}, fmt.Errorf(errorPrefix+"the route of %s: %w", req.account, err)
	}
	r := res.Route
	b := s.bank
	b.mu.Lock()
	defer b.mu.Unlock()
	act := b.active[r.Shard]
	deposits, ok := act.deposits[req.account]
	if !ok || act.token.Epoch != req.epoch {
		e := &router.NotOwnerError{
			Cell:        r.Cell,
			Shard:       r.Shard,
			Epoch:       req.epoch,
			Generation:  r.Generation,
			Incarnation: r.Incarnation,
		}
		if len(r.Replicas) > 0 {
			e.Hint = r.Replicas[0].Node
		}
		return depositResult{}, e
	}
	n := len(deposits) + 1
	deposits = append(deposits, fmt.Sprintf(depositFormat, n, b.cfg.node))
	key := string(depositsPrefix) + req.account
	writer := b.cfg.storage.Writer(act.token)
	err = b.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
		if cerr := writer.Check(ctx, tx); cerr != nil {
			//nolint:wrapcheck // Deposit wraps the error of the transaction.
			return cerr
		}
		tx.Put(key, strings.Join(deposits, " "))
		return nil
	})
	if err != nil {
		return depositResult{}, fmt.Errorf(errorPrefix+"a deposit of %s: %w", req.account, err)
	}
	act.deposits[req.account] = deposits
	return depositResult{deposit: n}, nil
}

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the deposits of each account.
	out io.Writer
	// clock reads the time of the store and of the processes.
	clock clock.Clock
	// rand is the source of randomness of the processes.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
}

// node is a process of the example, with its router and the server of its accounts.
type node struct {
	*embedded.Process
	// router is the router of the process.
	router *router.Watcher
	// server serves the deposits of the accounts of the node.
	server *server
}

// client deposits into the accounts through router.Do. It sends each deposit to the server of
// the writer of the route of the account, and counts the deposits that a server refused with
// a *router.NotOwnerError.
type client struct {
	// router routes the accounts.
	router router.Router
	// servers maps each node to its server. A deposit reads it after its first route, so the
	// program adds a server before it releases the deposits that wait.
	servers map[shard.NodeID]*server
	// redirects counts the deposits that a server refused with a *router.NotOwnerError.
	redirects atomic.Int64
}

// deposit deposits once into the account name through router.Do. The first call of the deposit
// calls before, and then calls the server of its route.
//
// Error modes:
//   - an error that wraps the error of router.Do, such as the error of the last call of the
//     deposit.
func (c *client) deposit(ctx context.Context, name string, before func()) error {
	first := true
	err := router.Do(ctx, c.router, []byte(name), attempts, func(r router.Route) error {
		if first {
			first = false
			before()
		}
		srv := c.servers[r.Replicas[0].Node]
		_, err := srv.Deposit(ctx, &depositRequest{account: name, epoch: r.Epoch})
		if _, ok := errors.AsType[*router.NotOwnerError](err); ok {
			c.redirects.Add(1)
		}
		return err
	})
	if err != nil {
		return fmt.Errorf(errorPrefix+"the deposit of %s: %w", name, err)
	}
	return nil
}

// round deposits once into every account at once, and returns once every deposit has
// returned. Each deposit calls before ahead of its first call of a server.
//
// Error modes:
//   - an error that joins the error of each deposit that failed.
func (c *client) round(ctx context.Context, before func()) error {
	errs := make([]error, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Go(func() { errs[i] = c.deposit(ctx, name, before) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// system is the store, the storage, the network and the partitioner that the processes of the
// example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the deposits.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
	// part maps the name of each account to its position.
	part partition.Partitioner
}

// start starts the process name under ctx with a router of the namespace, and returns once
// the process serves. A replica.Runner opens the tenures of the node through the bank of the
// process, and the network of s serves the process.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as the cause of ctx when ctx ends
//     before the process serves.
func (s *system) start(ctx context.Context, name shard.NodeID) (*node, error) {
	n := shard.Node{ID: name, Address: string(name) + port, Identity: shard.Identity(name)}
	b := newBank(&bankConfig{storage: s.storage, partitioner: s.part, node: n.ID})
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{
			Spec:    spec,
			Handler: replica.NewRunner(b),
			Guard:   s.storage,
			Route:   true,
		}},
		Store:      s.st,
		Membership: s.st.Membership(),
		Transport:  s.net.Local(protocol.Metadata{Caller: n.Identity, Version: protocol.Current}),
		Clock:      s.env.clock,
		Rand:       s.env.rand,
		Reporter:   noop.New(),
		Registry:   s.env.registry,
		Logger:     s.env.logger,
		Node:       n,
		Volatile:   true,
	})
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the process %s: %w", name, err)
	}
	s.net.AddAgent(namespace, n.ID, p.AgentServer())
	s.net.AddController(n.Address, p.ControllerServer())
	if err := p.Start(ctx); err != nil {
		return nil, fmt.Errorf(errorPrefix+"the start of %s: %w", name, err)
	}
	w, _ := p.Router(namespace)
	return &node{Process: p, router: w, server: &server{bank: b, router: w}}, nil
}

// moved runs the second round of deposits of c. Each deposit takes its route and waits. moved
// then starts node-b under procs, and waits until half of the shards have settled on node-b
// and the router of c has applied the moves. It then releases the deposits, and returns
// node-b once every deposit has returned. It returns nil for node-b when node-b did not
// start. The process of node-b stops when procs ends.
//
// Error modes:
//   - an error that wraps the error of the start of node-b, and of settle.
//   - an error that joins the error of each deposit that failed.
func (s *system) moved(ctx, procs context.Context, c *client) (*node, error) {
	gate := make(chan struct{})
	routed := make(chan struct{}, len(names))
	finished := make(chan struct{})
	var deposits error
	go func() {
		defer close(finished)
		deposits = c.round(ctx, func() {
			routed <- struct{}{}
			<-gate
		})
	}()
	for range names {
		select {
		case <-routed:
		case <-finished:
		}
	}
	b, err := s.start(procs, nodeB)
	if err == nil {
		c.servers[nodeB] = b.server
		err = settle(ctx, s.env.clock, s.st, c.router, nodeB, slots/2)
	}
	close(gate)
	<-finished
	return b, errors.Join(err, deposits)
}

// run runs the example in env over one in-memory store and one in-process network. It starts
// node-a, and deposits into every account three times. The deposits of the second round take
// their routes before node-b starts, and arrive at their servers once the shards that node-b
// takes have moved. run then writes one line per account to env.out with the deposits of the
// account, and a last line with the number of the deposits that router.Do routed again.
//
// The processes run under a context without the cancellation of ctx. run ends that context
// and waits for the processes before it returns.
//
// Error modes:
//   - an error that wraps the error of partitioner, such as config.ErrUnknown for a registry
//     without the built-in strategies.
//   - an error that wraps the error of the start of a process, such as embedded.ErrConfig for
//     an environment without a logger.
//   - an error that wraps the error of settle, of moved or of a deposit. A ctx that ends
//     before the deposits have returned ends run with such an error.
//   - an error that wraps the error of a write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	part, err := partitioner(env.registry, st)
	if err != nil {
		return err
	}
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork(), part: part}
	procs, finish := context.WithCancel(context.WithoutCancel(ctx))
	defer finish()
	a, err := sys.start(procs, nodeA)
	if err != nil {
		return err
	}
	defer func() {
		finish()
		<-a.Done()
	}()
	c := &client{router: a.router, servers: map[shard.NodeID]*server{nodeA: a.server}}
	err = settle(ctx, env.clock, st, a.router, nodeA, slots)
	if err == nil {
		err = c.round(ctx, func() {})
	}
	var b *node
	if err == nil {
		b, err = sys.moved(ctx, procs, c)
	}
	if b != nil {
		defer func() {
			finish()
			<-b.Done()
		}()
	}
	if err == nil {
		err = c.round(ctx, func() {})
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"the deposits: %w", err)
	}
	for _, name := range names {
		if _, werr := fmt.Fprintf(
			env.out,
			lineFormat,
			name,
			strings.Join(depositsOf(sys.storage, name), " "),
		); werr != nil {
			return fmt.Errorf(errorPrefix+"the deposits of %s: %w", name, werr)
		}
	}
	if _, werr := fmt.Fprintf(env.out, redirectsFormat, c.redirects.Load()); werr != nil {
		return fmt.Errorf(errorPrefix+"the redirects: %w", werr)
	}
	return nil
}

// partitioner resolves the spec of the namespace with reg and the capabilities of st, and
// returns the partitioner of the spec, which maps the name of each account to its position.
//
// Error modes:
//   - an error that wraps the error of config.Resolve, such as config.ErrUnknown for a
//     registry without the built-in strategies.
//   - an error that wraps the error of config.Build.
func partitioner(reg *config.Registry, st *memory.Store) (partition.Partitioner, error) {
	s := spec
	res, err := config.Resolve(reg, &s, map[config.Axis][]config.Capability{
		config.Store:      st.Provides(),
		config.Membership: st.Membership().Provides(),
	})
	var part partition.Partitioner
	if err == nil {
		part, err = config.Build[partition.Partitioner](reg, res, config.Partitioning)
	}
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the partitioner of %s: %w", namespace, err)
	}
	return part, nil
}

// settle waits on clk until node is the writer of want shards of the one cell of the namespace,
// and no transition of the cell is in flight. It then waits until w has applied that
// generation of the cell, so w routes each account to the writer of its shard.
//
// Error modes:
//   - an error that wraps errNoCell for a root scope that does not list one cell.
//   - an error that wraps the error of a read of the root scope or of the scope of the cell.
//   - an error that wraps the error of Router.Refresh.
//   - the cause of ctx when ctx ends first.
func settle(ctx context.Context, clk clock.Clock, st store.Reader, w router.Router, node shard.NodeID, want int) error {
	for {
		root, cur, err := st.Read(ctx, shard.Scope{Namespace: namespace}, shard.NamespaceIncarnation{}, store.Query{},
			page.Page{})
		var cells []shard.Placement
		if err == nil {
			cells, err = placements(ctx, cur)
		}
		if err == nil && len(cells) != 1 {
			err = errNoCell
		}
		if err != nil {
			return fmt.Errorf(errorPrefix+"the cells of %s: %w", namespace, err)
		}
		inc := root.Head.Incarnation
		cell := shard.CellID(ulid.Format(cells[0].Shard.ID()))
		state, cur, err := st.Read(ctx, shard.Scope{Namespace: namespace, Cell: cell}, inc, store.Query{Node: node},
			page.Page{})
		var onNode []shard.Placement
		if err == nil {
			onNode, err = placements(ctx, cur)
		}
		if err != nil {
			return fmt.Errorf(errorPrefix+"the placements of cell %s: %w", cell, err)
		}
		if len(state.Transitions) == 0 && len(onNode) == want {
			_, err := w.Refresh(ctx, &router.RefreshRequest{Cell: cell, Min: state.Head.Generation, Incarnation: inc})
			if err != nil {
				return fmt.Errorf(errorPrefix+"the refresh of cell %s: %w", cell, err)
			}
			return nil
		}
		if clock.Wait(ctx, clk, tick) != nil {
			return context.Cause(ctx)
		}
	}
}

// placements returns the placements of the page of cur, and closes cur.
//
// Error modes:
//   - an error that joins the error of the iteration of cur and the error of its Close.
func placements(ctx context.Context, cur page.Cursor[shard.Placement]) ([]shard.Placement, error) {
	var ps []shard.Placement
	var err error
	for p, perr := range cur.Seq(ctx) {
		if perr != nil {
			err = perr
			break
		}
		ps = append(ps, p)
	}
	return ps, errors.Join(err, cur.Close())
}

// depositsOf returns the deposits of the account that r contains: the entry of each deposit of
// the account, in order. An account without a deposit has no entry.
func depositsOf(r fencedstore.Reader, account string) []string {
	list, _ := r.Get(string(depositsPrefix) + account)
	return strings.Fields(list)
}

// main runs the example until every deposit has returned or the process receives an
// interrupt. After an error, it writes the error to os.Stderr and exits with the status 1.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	reg := config.NewRegistry(builtin.Support())
	err := builtin.Register(reg)
	if err == nil {
		err = run(ctx, &environment{
			out:      os.Stdout,
			clock:    hlc.New(1),
			rand:     crypto.New(),
			registry: reg,
			logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		})
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitFailure)
	}
}
