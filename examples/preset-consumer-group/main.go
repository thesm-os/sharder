// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

// Command preset-consumer-group runs the consumers of a queue under the preset consumer-group.
// The spec of the namespace has one slot per partition of the queue, so each shard is one
// partition. Each consumer consumes the partitions of its shards, and commits the offset of
// each message through the fence of the tenure of its partition.
//
// The program runs two application processes, consumer-a and consumer-b, in one OS process,
// over one in-memory store and one in-process network. Each process is an embedded.Process,
// which runs the controller of the process and the agent of its node. The consumer of a
// process opens each tenure of its node for a replica.Runner. The run has three stages:
//
//   - consumer-a starts alone, and the controller places the 4 partitions of the queue on it.
//   - Once consumer-a has consumed a message of every partition, consumer-b starts. The
//     controller moves half of the partitions to consumer-b, which starts each of them at the
//     offset that consumer-a committed.
//   - Once half of the messages have been consumed, consumer-b closes. Its partitions move
//     back to consumer-a, which consumes the remaining messages.
//
// The program then writes one line per partition with the log of the partition:
//
//	partition-2: 1:consumer-a 2:consumer-b 3:consumer-b 4:consumer-b 5:consumer-a 6:consumer-a 7:consumer-a 8:consumer-a
//
// Run it from the examples module:
//
//	go run ./preset-consumer-group
//
// The program logs the errors of the processes at the level error.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/crypto"
	"go.thesmos.sh/core/telemetry/noop"

	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/embedded"
	"go.thesmos.sh/sharder/examples/internal/fencedstore"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/replica"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store/memory"
)

// The namespace of the example.
const (
	// namespace is the namespace of the consumers.
	namespace shard.Namespace = "events"
	// slotsConfig is the configuration of fixed-slots in the spec of the namespace: one slot per
	// partition.
	slotsConfig = `{"slots":4}`
	// churnConfig lets one pass of the planner move half of the partitions of the cell and 4
	// partitions out of one node. The preset lets a pass move 9% of the shards of a cell. For
	// the 4 partitions of the example, that is one partition per pass.
	churnConfig = `{"churn":{"maxShardFraction":0.5,"perNode":4}}`
)

// The processes of the example.
const (
	// consumerA is the process that starts first and runs until every message has been
	// consumed.
	consumerA = "consumer-a"
	// consumerB is the process that starts once every partition has a consumed message, and
	// closes once half of the messages have been consumed.
	consumerB = "consumer-b"
)

// port is the port of the address of each process.
const port = ":7000"

// The queue of the example.
const (
	// partitions is the number of the partitions of the queue.
	partitions = 4
	// messages is the number of the messages of each partition.
	messages = 8
	// tick is the time that the consumption of one message takes.
	tick = 50 * time.Millisecond
)

// drainFor bounds the move of the partitions of consumer-b.
const drainFor = time.Minute

// The output and the failure of the program.
const (
	// lineFormat formats the line of one partition: the number of the partition and its log.
	lineFormat = "partition-%d: %s\n"
	// errorPrefix starts the text of each error of the program.
	errorPrefix = "preset-consumer-group: "
	// exitFailure is the exit status of the program after an error.
	exitFailure = 1
)

// The formats of the entries of the storage of the consumers.
const (
	// messageFormat formats the entry of one message in the log of its partition: the number
	// of the message and the node that consumed it.
	messageFormat = "%d:%s"
	// assignmentFormat formats the entry of one tenure in the assignments of its partition: the
	// node and the epoch of the tenure.
	assignmentFormat = "%s@%d"
)

// spec is the initial spec of the namespace. It selects the preset consumer-group with one
// slot per partition, and lets the planner move up to half of the partitions in one pass.
var spec = config.Spec{
	Namespace:    namespace,
	Preset:       builtin.ConsumerGroup,
	Partitioning: config.Selection{Name: fixedslots.Name, Config: json.RawMessage(slotsConfig)},
	Placement:    config.Selection{Config: json.RawMessage(churnConfig)},
}

// prefix is the prefix of a key of the storage of the consumers.
type prefix string

// The prefixes of the keys of the storage of the consumers. The number of a partition ends
// each key.
const (
	// offsetPrefix starts the key of the offset of a partition: the number of the messages of
	// the partition that the consumers have committed.
	offsetPrefix prefix = "offset/"
	// logPrefix starts the key of the log of a partition. The log lists the messages of the
	// partition that the consumers have consumed, in order, separated by spaces.
	logPrefix prefix = "log/"
	// assignmentsPrefix starts the key of the assignments of a partition. The assignments list
	// the tenures of the partition in order, separated by spaces.
	assignmentsPrefix prefix = "assignments/"
)

// cursor is the next message of a partition.
type cursor struct {
	// partition is the number of the partition.
	partition int
	// offset is the number of the messages of the partition that the consumers have
	// committed.
	offset int
}

// consumerConfig is the configuration of a consumer.
type consumerConfig struct {
	// storage is the storage of the offsets and the logs. The agent of the node acquires its
	// fence for each tenure.
	storage *fencedstore.Store
	// clock times the consumption of the messages.
	clock clock.Clock
	// node is the node of the consumer.
	node shard.NodeID
	// messages is the number of the messages of each partition.
	messages int
	// tick is the time that the consumption of one message takes.
	tick time.Duration
}

// consumer opens the tenures of the shards of its node for a replica.Runner, and consumes the
// messages of the partition of each tenure. The spec of the namespace has one slot per
// partition, so each shard is one partition.
//
// The work of a tenure starts at the offset that the consumers of the partition have
// committed. It consumes one message per tick. It appends each message to the log of its
// partition and commits the next offset, in one transaction that checks the fence with the
// token of the tenure. Open reads the offset from a state that contains the acquisition of
// the fence, so a new consumer of a partition starts at the offset of the old consumer. The
// fence refuses the writes of an earlier tenure from the acquisition on, so each message is
// consumed once.
//
// # Concurrency
//
// A consumer is safe for concurrent use.
type consumer struct {
	// replica is the replica of the storage that Open reads when the replica has applied the
	// acquisition of the tenure.
	replica *fencedstore.Replica
	// cfg is the configuration of the consumer.
	cfg consumerConfig
}

var _ replica.Opener = (*consumer)(nil)

// newConsumer returns the consumer of cfg, with a new replica of the storage of cfg.
func newConsumer(cfg *consumerConfig) *consumer {
	return &consumer{replica: cfg.storage.NewReplica(), cfg: *cfg}
}

// Open appends the tenure of req to the assignments of each partition of its shard, in one
// transaction with the key of the add, so an add that the consumer receives again does not
// append the tenure twice. It then reads the offset of each partition through Store.Reader,
// and returns the consumption of the partitions as the work of the tenure.
//
// Error modes:
//   - an error that wraps the error of the transaction, such as epoch.ErrFenced for a tenure
//     that a later tenure has fenced, and the cause of ctx.
//   - an error that wraps strconv.ErrSyntax for an offset that is not a number.
func (c *consumer) Open(ctx context.Context, req *replica.OpenRequest) (replica.OpenResult, error) {
	r, t := req.Request, req.Token
	ps := c.partitionsOf(r.Range)
	key := r.Key()
	entry := fmt.Sprintf(assignmentFormat, c.cfg.node, t.Epoch)
	writer := c.cfg.storage.Writer(t)
	err := c.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
		if tx.Recorded(&key) {
			return nil
		}
		if err := writer.Check(ctx, tx); err != nil {
			//nolint:wrapcheck // Open wraps the error of the transaction.
			return err
		}
		for _, p := range ps {
			k := string(assignmentsPrefix) + strconv.Itoa(p)
			list, _ := tx.Get(k)
			tx.Put(k, strings.TrimSpace(list+" "+entry))
		}
		tx.Record(&key)
		return nil
	})
	if err != nil {
		return replica.OpenResult{}, fmt.Errorf(errorPrefix+"the tenure of partitions %v at epoch %d: %w", ps, t.Epoch,
			err)
	}
	src := c.cfg.storage.Reader(&t, c.replica)
	cursors := make([]cursor, 0, len(ps))
	for _, p := range ps {
		offset, err := offsetOf(src, p)
		if err != nil {
			return replica.OpenResult{}, err
		}
		cursors = append(cursors, cursor{partition: p, offset: offset})
	}
	return replica.OpenResult{Run: func(ctx context.Context) error { return c.run(ctx, writer, cursors) }}, nil
}

// partitionsOf returns the numbers of the partitions whose positions r contains, in
// increasing order. The position of a partition is its number as 4 bytes in big-endian
// order, which is the position of the slot of that number.
func (*consumer) partitionsOf(r shard.Range) []int {
	var ps []int
	for p := range partitions {
		if r.Contains(binary.BigEndian.AppendUint32(nil, uint32(p))) {
			ps = append(ps, p)
		}
	}
	return ps
}

// run consumes the messages of the partitions of cursors through writer, one message per
// tick, until every message of the partitions has been consumed. Each message appends its
// number and the node to the log of its partition, and commits the next offset.
//
// Error modes:
//   - an error that wraps the error of clock.Wait once ctx ends.
//   - an error that wraps the error of the transaction of a message, such as epoch.ErrFenced
//     once a later tenure has acquired the fence.
func (c *consumer) run(ctx context.Context, writer fence.Writer[*fencedstore.Tx], cursors []cursor) error {
	for pending := true; pending; {
		pending = false
		for i := range cursors {
			cur := &cursors[i]
			if cur.offset == c.cfg.messages {
				continue
			}
			pending = true
			next := cur.offset + 1
			if err := clock.Wait(ctx, c.cfg.clock, c.cfg.tick); err != nil {
				return fmt.Errorf(errorPrefix+"the wait for message %d of partition %d: %w", next, cur.partition, err)
			}
			part := strconv.Itoa(cur.partition)
			entry := fmt.Sprintf(messageFormat, next, c.cfg.node)
			err := c.cfg.storage.Update(ctx, func(tx *fencedstore.Tx) error {
				if err := writer.Check(ctx, tx); err != nil {
					//nolint:wrapcheck // run wraps the error of the transaction.
					return err
				}
				list, _ := tx.Get(string(logPrefix) + part)
				tx.Put(string(logPrefix)+part, strings.TrimSpace(list+" "+entry))
				tx.Put(string(offsetPrefix)+part, strconv.Itoa(next))
				return nil
			})
			if err != nil {
				return fmt.Errorf(errorPrefix+"message %d of partition %d: %w", next, cur.partition, err)
			}
			cur.offset = next
		}
	}
	return nil
}

// environment is what the program takes from its OS process.
type environment struct {
	// out receives the log of each partition.
	out io.Writer
	// clock reads the time of the store and of the processes, and times the consumption.
	clock clock.Clock
	// rand is the source of randomness of the processes.
	rand rand.Rand
	// registry builds the strategies of the namespace. It contains the built-in strategies and
	// presets.
	registry *config.Registry
	// logger logs the errors that do not stop a process.
	logger *slog.Logger
}

// system is the store, the storage and the network that the processes of the example share.
type system struct {
	// env is the environment of the program.
	env *environment
	// st is the store and the membership of the namespace.
	st *memory.Store
	// storage is the storage of the offsets and the logs.
	storage *fencedstore.Store
	// net connects the processes.
	net *local.Network
}

// start starts the process name under ctx, and returns once the process serves. The consumer
// of the process consumes the partitions of its tenures through a replica.Runner, and the
// network of s serves the process.
//
// Error modes:
//   - an error that wraps the error of embedded.New, such as embedded.ErrConfig for an
//     environment without a logger.
//   - an error that wraps the error of Process.Start, such as config.ErrUnknown for a
//     registry without the built-in strategies, or the cause of ctx when ctx ends before the
//     process serves.
func (s *system) start(ctx context.Context, name string) (*embedded.Process, error) {
	node := shard.Node{ID: shard.NodeID(name), Address: name + port, Identity: shard.Identity(name)}
	c := newConsumer(&consumerConfig{
		storage:  s.storage,
		clock:    s.env.clock,
		node:     node.ID,
		messages: messages,
		tick:     tick,
	})
	p, err := embedded.New(&embedded.Config{
		Namespaces: []embedded.Namespace{{Spec: spec, Handler: replica.NewRunner(c), Guard: s.storage}},
		Store:      s.st,
		Membership: s.st.Membership(),
		Transport:  s.net.Local(protocol.Metadata{Caller: node.Identity, Version: protocol.Current}),
		Clock:      s.env.clock,
		Rand:       s.env.rand,
		Reporter:   noop.New(),
		Registry:   s.env.registry,
		Logger:     s.env.logger,
		Node:       node,
		Volatile:   true,
	})
	if err != nil {
		return nil, fmt.Errorf(errorPrefix+"the process %s: %w", name, err)
	}
	s.net.AddAgent(namespace, node.ID, p.AgentServer())
	s.net.AddController(node.Address, p.ControllerServer())
	if err := p.Start(ctx); err != nil {
		return nil, fmt.Errorf(errorPrefix+"the start of %s: %w", name, err)
	}
	return p, nil
}

// run runs the example in env over one in-memory store and one in-process network. It starts
// consumer-a, and starts consumer-b once every partition has a consumed message. Once half of
// the messages have been consumed, it closes consumer-b, whose partitions move back to
// consumer-a. Once every message has been consumed, run stops consumer-a. It then writes one
// line per partition to env.out with the log of the partition.
//
// Error modes:
//   - an error that wraps the error of the start of a process, such as config.ErrUnknown for
//     a registry without the built-in strategies.
//   - an error that wraps the cause of ctx when ctx ends before every message has been
//     consumed.
//   - an error that wraps the error of Process.Close for consumer-b.
//   - an error that wraps the error of a write to env.out.
func run(ctx context.Context, env *environment) (err error) {
	st := memory.New(env.clock)
	defer func() { err = errors.Join(err, st.Close()) }()
	sys := &system{env: env, st: st, storage: fencedstore.New(), net: local.NewNetwork()}
	procs, finish := context.WithCancel(ctx)
	defer finish()
	a, err := sys.start(procs, consumerA)
	if err != nil {
		return err
	}
	defer func() {
		finish()
		<-a.Done()
	}()
	total := partitions * messages
	err = await(ctx, env.clock, func() bool { return begun(sys.storage) })
	var b *embedded.Process
	if err == nil {
		b, err = sys.start(procs, consumerB)
	}
	if err == nil {
		err = await(ctx, env.clock, func() bool { return consumed(sys.storage) >= total/2 })
		drain, cancel := context.WithTimeout(ctx, drainFor)
		err = errors.Join(err, b.Close(drain))
		cancel()
	}
	if err == nil {
		err = await(ctx, env.clock, func() bool { return consumed(sys.storage) == total })
	}
	if err != nil {
		return fmt.Errorf(errorPrefix+"%d of %d messages have been consumed: %w", consumed(sys.storage), total, err)
	}
	for p := range partitions {
		if _, werr := fmt.Fprintf(env.out, lineFormat, p, strings.Join(logOf(sys.storage, p), " ")); werr != nil {
			return fmt.Errorf(errorPrefix+"the log of partition %d: %w", p, werr)
		}
	}
	return nil
}

// await waits on clk one tick at a time until done reports true. It returns the cause of ctx
// when ctx ends first.
func await(ctx context.Context, clk clock.Clock, done func() bool) error {
	for !done() {
		if clock.Wait(ctx, clk, tick) != nil {
			return context.Cause(ctx)
		}
	}
	return nil
}

// offsetOf returns the offset of the partition p that r contains: the number of the messages
// of p that the consumers have committed. A partition without a commit has the offset 0.
//
// Error modes:
//   - an error that wraps strconv.ErrSyntax for an offset that is not a number.
func offsetOf(r fencedstore.Reader, p int) (int, error) {
	v, ok := r.Get(string(offsetPrefix) + strconv.Itoa(p))
	if !ok {
		return 0, nil
	}
	offset, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf(errorPrefix+"the offset of partition %d: %w", p, err)
	}
	return offset, nil
}

// logOf returns the log of the partition p that r contains: the entry of each message of p
// that a consumer has consumed, in order. A partition without a consumed message has an empty
// log.
func logOf(r fencedstore.Reader, p int) []string {
	list, _ := r.Get(string(logPrefix) + strconv.Itoa(p))
	return strings.Fields(list)
}

// begun reports whether the log of every partition in storage has a message.
func begun(storage *fencedstore.Store) bool {
	for p := range partitions {
		if len(logOf(storage, p)) == 0 {
			return false
		}
	}
	return true
}

// consumed returns the number of the messages that the logs of the partitions in storage
// list.
func consumed(storage *fencedstore.Store) int {
	n := 0
	for p := range partitions {
		n += len(logOf(storage, p))
	}
	return n
}

// main runs the example until every message has been consumed or the process receives an
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
