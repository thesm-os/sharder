// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.dokimi.dev/sim"
	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/clock/fake"
	"go.thesmos.sh/core/clock/hlc"
	"go.thesmos.sh/core/coretest/telemetrytest"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/rand"
	"go.thesmos.sh/core/rand/seeded"
	"go.thesmos.sh/core/tag"
	"go.thesmos.sh/core/telemetry"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/builtin"
	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/config/configtest"
	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/internal/machine"
	"go.thesmos.sh/sharder/internal/machine/agent"
	"go.thesmos.sh/sharder/internal/machine/cell"
	"go.thesmos.sh/sharder/internal/machine/root"
	"go.thesmos.sh/sharder/internal/machine/routes"
	"go.thesmos.sh/sharder/ownership"
	"go.thesmos.sh/sharder/partition"
	"go.thesmos.sh/sharder/partition/fixedslots"
	"go.thesmos.sh/sharder/placement"
	"go.thesmos.sh/sharder/placement/constraint"
	"go.thesmos.sh/sharder/placement/planner"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/replication"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
	"go.thesmos.sh/sharder/store/memory"
)

// world is the world of the machines of a namespace, process one of its processes, outcome the
// outcome of one of their effects, and action an action of a simulation of the world.
type (
	world   = sim.World[machine.Event, machine.Effect]
	process = sim.Process[machine.Event, machine.Effect]
	outcome = sim.Outcome[machine.Event, machine.Effect]
	action  = sim.Action[machine.Event, machine.Effect]
)

// The bounds of the inputs of a system, which each case chooses.
const (
	// maxHosts is the most hosts of a case.
	maxHosts = 5
	// maxNamespaces is the most namespaces of a case. The names come from namespaces.
	maxNamespaces = 2
	// maxSlots is the most slots of the partitioning of a namespace whose preset selects
	// fixed-slots.
	maxSlots = 16
)

// The facts of each host and namespace of a world.
const (
	// hostPrefix is the prefix of the name of each host, before its position from 1.
	hostPrefix = "host-"
	// port is the port of the address of the controller of each host.
	port = ":7000"
	// operator is the identity of the operator of each namespace.
	operator shard.Identity = "operator"
	// storeNode is the node of the clock of the store.
	storeNode clock.NodeID = math.MaxUint32
	// slotsKey is the key of the slot count in the configuration of fixed-slots.
	slotsKey = "slots"
)

// driftRate is the maxDriftRate of the lease of each namespace of a simulation. It keeps the
// deadline of each node before the expiry of its lease at the store for every drift of a world.
const driftRate = 0.5

// firstVersion is the version of the spec of each namespace that a system builds. A spec edit of
// the action submit raises it.
const firstVersion epoch.Epoch = 1

// plantedTokens is the token after which a planted defect numbers the effects that it adds to a
// machine. No machine of a run takes that many tokens.
const plantedTokens machine.Token = math.MaxUint64 / 2

// modes is the generator of the mode of a loss of the lease of a namespace.
var modes = prop.SampledFrom(ownership.ContinueFenced, ownership.Stop)

// The zones of the nodes of a world. Each node has the label zoneLabel with one of them.
const (
	// zoneLabel is the key of the label of the zone of a node.
	zoneLabel = "zone"
	// zoneEast is the zone whose nodes take replicas.
	zoneEast = "east"
	// zoneWest is the zone whose nodes the constraint of each spec refuses.
	zoneWest = "west"
)

// zoneRules are the rules of the constraint labels of each spec of a simulation: no replica
// runs on a node of the zone west.
var zoneRules = []constraint.Rule{{Key: zoneLabel, Operator: constraint.NotIn, Values: []string{zoneWest}}}

// namespaces lists the namespaces of a system, in the order in which a case adds them.
var namespaces = []shard.Namespace{"orders", "payments"}

// presets is the generator of the preset of the namespaces of a system: one of the built-in
// presets.
var presets = prop.Composite(func(c *prop.Case) config.PresetName {
	all := builtin.Presets()
	return all[c.Draw(prop.Integer(0, len(all)-1), "index")].Name
})

// provided lists the capabilities of the store and the membership of a world, which the
// memory store does not declare.
var provided = map[config.Axis][]config.Capability{
	config.Store:      {config.Durable},
	config.Membership: {config.Leases},
}

// role is what a process of a host runs for one namespace.
type role string

// The roles of the processes of a host.
const (
	// roleRoot is the root of the namespace.
	roleRoot role = "root"
	// roleRootAgent is the agent of the controller process in the root scope, which starts and
	// stops the cell machines that the root places on the host.
	roleRootAgent role = "root-agent"
	// roleAgent is the agent of the node in its cell, with the reference application.
	roleAgent role = "agent"
	// roleRouter is the router of the namespace.
	roleRouter role = "router"
	// roleCell is a cell machine that the agent of the root has started.
	roleCell role = "cell"
)

// spawned lists the roles that each host spawns for each namespace, in the order of their
// spawns. A restart of the host starts them again, and no cell machine.
var spawned = []role{roleRoot, roleRootAgent, roleAgent, roleRouter}

// address is the name of a process of a world: the host that runs it, its namespace, its role
// and, for a cell machine, its cell.
type address struct {
	// host is the name of the host.
	host string
	// namespace is the namespace of the process.
	namespace shard.Namespace
	// role is the role of the process.
	role role
	// cell is the cell of a cell machine, and empty for every other role.
	cell shard.CellID
}

// String returns the name of the process: the host, the namespace and the role, joined by
// slashes, and the cell of a cell machine after them.
func (a address) String() string {
	name := a.host + "/" + string(a.namespace) + "/" + string(a.role)
	if a.cell != "" {
		name += "/" + string(a.cell)
	}
	return name
}

// procKey is one epoch of a process of a world.
type procKey struct {
	// process is the name of the process.
	process string
	// epoch is the epoch of the process.
	epoch uint64
}

// variant is the part of a system that a planted defect or the inputs of a trace replace. The
// zero variant is the system of sharder, whose case chooses every input.
type variant struct {
	// cell returns the machine that a host of s runs for the cell machine m, and is nil for
	// the cell machine itself.
	cell func(s *env, m *cell.Machine) machine.Machine
	// agent returns the machine that a host of s runs for the agent m of a cell, and is nil for
	// the agent itself.
	agent func(s *env, m *agent.Machine) machine.Machine
	// rootAgent returns the machine that a host of s runs for the agent m of the root, and is nil
	// for the agent itself.
	rootAgent func(s *env, m *agent.Machine) machine.Machine
	// router returns the machine that a host of s runs for the router m, and is nil for the
	// router itself.
	router func(s *env, m *routes.Machine) machine.Machine
	// cellReporter returns the reporter of a cell machine of s over the recorder r of its host,
	// and is nil for the recorder itself.
	cellReporter func(s *env, r *telemetrytest.Recorder) telemetry.Reporter
	// writer returns the writer of the commit f to a scope with the header before, and is nil
	// for the writer of the store.
	writer func(s *env, f *machine.Commit, before store.Head) store.Writer
	// crowd is the zone of the one host of a crowded system, and empty for a system whose case
	// chooses its hosts. A crowded system starts with one host of the zone crowd and the most
	// slots in each namespace, so the first placement puts every shard of a namespace on the node
	// of that host.
	crowd string
	// sources is the maxSourceShards of the planner of each spec, and the default of the planner
	// for 0.
	sources int
	// unfenced makes the fence of the reference application report each acquisition as done
	// without an acquisition.
	unfenced bool
	// schemaless makes each header that a Head reads have at most the schema that this
	// release writes.
	schemaless bool
	// failedRenewals makes a renewal of a session that fails move the deadline of the session.
	failedRenewals bool
	// tight bounds the transitions in flight, the transitions of a node and the callbacks of a
	// node of each namespace to one.
	tight bool
	// unreported makes the runtime of each planning pass drop the violations of the pass.
	unreported bool
	// stop makes ownership.Stop the mode of a loss of the lease of each namespace, in place of a
	// mode that the case chooses.
	stop bool
	// unconstrained leaves the constraint of the zones out of the first version of each spec.
	// The first spec edit of the action submit adds it.
	unconstrained bool
	// paired runs both namespaces of namespaces, in place of a number that the case chooses.
	paired bool
}

// joinedSession is a session that the membership granted to a Join: its scope, the namespace
// incarnation of the Join, and the incarnation of the header of the scope at the grant.
type joinedSession struct {
	// scope is the scope of the session.
	scope shard.Scope
	// incarnation is the namespace incarnation of the Join.
	incarnation shard.NamespaceIncarnation
	// current is the incarnation of the header of the scope at the grant, and zero for a scope
	// that no commit has created.
	current shard.NamespaceIncarnation
}

// nodeKey is one incarnation of a node in the membership of one scope.
type nodeKey struct {
	// scope is the scope of the membership.
	scope shard.Scope
	// node is the node.
	node shard.NodeID
	// incarnation is the incarnation of the node in the membership of the scope.
	incarnation epoch.Epoch
}

// commit is a commit that the store applied: its scope, its writer, the generation that it
// names, its mutation, the header of its scope before it, and the generation that it created.
type commit struct {
	// mutation is a copy of the mutation of the commit.
	mutation *store.Mutation
	// scope is the scope of the commit.
	scope shard.Scope
	// before is the header of the scope before the commit.
	before store.Head
	// incarnation is the namespace incarnation of the writer of the commit.
	incarnation shard.NamespaceIncarnation
	// term is the epoch of the writer of the commit.
	term epoch.Epoch
	// expect is the generation that the commit names as the one that it replaces.
	expect epoch.Epoch
	// generation is the generation that the commit created.
	generation epoch.Epoch
}

// callback is a callback that a controller sent: its request, the identity of the host of the
// controller, the number of the commits that the store had applied when the controller sent
// it, and whether the controller has taken a reply without an error.
type callback struct {
	// request is a copy of the request of the callback.
	request *protocol.Request
	// caller is the identity of the host of the controller.
	caller shard.Identity
	// commits is the number of the commits that the store had applied when the controller
	// sent the callback.
	commits int
	// replied reports that the controller has taken a reply to the callback without an error.
	replied bool
}

// acquisition is an acquisition of a fence that the fence reported to an agent as done, and
// the number of the records of the reference application at the report.
type acquisition struct {
	// token is the token of the acquisition.
	token fence.Token
	// records is the number of the records of the reference application at the report.
	records int
}

// env is the environment that the processes of one world share: the case of the world, the
// store with its membership, the reference application, the registry, the spec of each
// namespace with what the oracles read from it, the hosts, the waits of the effects in flight,
// the records of the commits, the callbacks, the acquisitions, the sessions and the forged
// callbacks, and the state of each invariant.
//
// # Concurrency
//
// An env is not safe for concurrent use. The world calls its machines and its performers on
// the goroutine of its case.
type env struct {
	// tb is the case of the world, which fails at a config that a constructor refuses.
	tb assert.TB
	// timers is the timers of the physical clocks of the world, which report a fault to tb.
	timers timerless
	// store keeps the scopes of each namespace, in its manual mode over the time of the run.
	store *memory.Store
	// members is the membership of store.
	members *memory.Membership
	// app is the fenced store of the reference application, which every host shares.
	app *FencedStore
	// registry builds the strategies of each spec.
	registry *config.Registry
	// specs lists the spec of each namespace, in the order of namespaces.
	specs []config.Spec
	// edit is the configuration of the planner that each spec edit of the action submit
	// selects: the constraint of the zones, with the search of the variant.
	edit json.RawMessage
	// leases maps each namespace to the lease of its ownership, which is the lease of the
	// session of each campaign in the namespace.
	leases map[shard.Namespace]ownership.Lease
	// roles maps each namespace to the serving roles of the state model of its spec.
	roles map[shard.Namespace][]replication.Role
	// budgets maps each namespace to the budget of its spec, with the defaults that
	// config.Resolve fills in.
	budgets map[shard.Namespace]config.Budget
	// partitioners maps each namespace to the partitioner of its spec, which computes the
	// position of a key.
	partitioners map[shard.Namespace]partition.Partitioner
	// probes maps each namespace to one key of each position of its layout, in the order of
	// the keys, for the oracles that read the routes of the routers.
	probes map[shard.Namespace][][]byte
	// modes maps each namespace to the mode of a loss of the lease of its spec.
	modes map[shard.Namespace]ownership.OnLeaseLoss
	// layouts maps each namespace to the number of the shards of the layout of its cell.
	layouts map[shard.Namespace]int
	// hosts lists the hosts in the order of their additions.
	hosts []*host
	// waits is the state of the waits of the effects in flight.
	waits waits
	// variant is the part of the system that a planted defect replaces.
	variant variant
	// commits lists each commit that the store applied, in order.
	commits []commit
	// callbacks lists each callback that a performer has sent, in order. The action forge
	// sends a variant of one of them.
	callbacks []callback
	// acquisitions lists each acquisition that the fence reported to an agent as done, in
	// order.
	acquisitions []acquisition
	// joined lists each session that the membership granted to a Join, in order.
	joined []joinedSession
	// forged contains the request of each callback that the action forge sent.
	forged map[*protocol.Request]bool
	// labels maps each incarnation of a node in the membership of a cell to the labels with
	// which the node joined.
	labels map[nodeKey]tag.Tags
	// epochs is the state of the invariant of epoch uniqueness.
	epochs epochs
	// fences is the state of the invariant of writer safety.
	fences fences
	// assignments is the state of the invariant of role safety.
	assignments assignments
	// admissions is the state of the invariant of admission.
	admissions assignments
	// constraints is the state of the invariant of constraints.
	constraints assignments
	// reporting is the state of the oracle of the violations that the planning passes report.
	reporting reporting
	// keeping is the state of the oracle of the effects of callbacks.
	keeping keeping
	// coverage is the state of the oracle of the tenures.
	coverage coverage
	// settling is the state of the oracle of unknown outcomes.
	settling settling
	// isolation is the state of the oracle of namespace incarnations.
	isolation isolation
	// bounding is the state of the oracle of the bounds on work in flight.
	bounding bounding
	// ordering is the state of the oracle of order.
	ordering ordering
	// outages is the state of the oracle of outages.
	outages outages
	// deadlines is the state of the oracle of sessions.
	deadlines deadlines
	// counting is the state of the oracle of the counts.
	counting counting
	// catalog is the shards of each namespace that the oracles of serving read.
	catalog catalog
	// disruptions lists each disruption that the action disrupt has requested, in order. The
	// action done ends the reservation of one of them.
	disruptions []budget.Disruption
	// writes lists the open write transactions of the action write, in the order of their
	// begins.
	writes []*Txn
	// sightings is what the routers and the agents served at the last sight of the oracles.
	sightings sightings
	// ids counts the IDs that the actions have given proposals and disruptions.
	ids uint64
	// controls is the number of the commits that the invariant of control safety has read.
	controls int
	// forgeries is the number of the entries of the log that the oracle of forged callbacks
	// has read.
	forgeries int
	// schemas is the number of the commits that the oracle of schemas has read.
	schemas int
}

// host is one process of the topology embedded: a node, a clock, a source of randomness, a
// reporter and a logger, which every machine of the host shares, and the machines that the
// host runs.
//
// # Concurrency
//
// A host is not safe for concurrent use, as its env is not.
type host struct {
	// sys is the env of the host.
	sys *env
	// clock is the clock of every machine of the current incarnation of the host, over the
	// clock of the host in the world. The first machine of each incarnation builds it.
	clock clock.Clock
	// rand is the source of randomness of every machine of the host.
	rand rand.Rand
	// reporter records the instruments of every machine of the current incarnation of the host,
	// which the oracles read. The first machine of each incarnation builds it, as a process
	// builds its reporter when it starts.
	reporter *telemetrytest.Recorder
	// logger logs the errors that do not stop a machine of the host, and discards them.
	logger *slog.Logger
	// parts maps each namespace to the latest machines that the host spawned for it.
	parts map[shard.Namespace]*part
	// cells maps the name of each cell machine process that the agent of the root has started
	// to the state of its last start.
	cells map[string]*hosted
	// name is the name of the host in the world, which is the ID of its node.
	name string
	// node is the node of the host. Its incarnation is the one that the membership gives it.
	node shard.Node
	// index is the position of the host in the hosts of its env, from 1. It is the node of
	// the clock of the host.
	index int
}

// part is the latest machines that a host spawned for one namespace. A restart of the host
// replaces each of them.
type part struct {
	// root is the root of the namespace.
	root *root.Machine
	// rootAgent is the agent of the controller process in the root scope.
	rootAgent *agent.Machine
	// agent is the agent of the node in its cell.
	agent *agent.Machine
	// router is the router of the namespace.
	router *routes.Machine
}

// hosted is the last start of a cell machine on a host: its scope, its term, the epoch of the
// agent of the root that started it, its machine and the reply of the machine to Started.
type hosted struct {
	// err is the error of the reply of the machine to Started, once ready is true.
	err error
	// machine is the cell machine, and nil until the world starts it.
	machine *cell.Machine
	// scope is the scope of the cell.
	scope shard.Scope
	// term is the term of the cell machine, which the add of its cell names.
	term epoch.Epoch
	// by is the epoch of the agent of the root that started the machine. A crash of the host
	// ends every machine of the host, so a machine of an earlier epoch of the agent does not
	// run.
	by uint64
	// started is the token of the Started of the machine.
	started machine.ReplyToken
	// ready reports that the machine has replied to Started.
	ready bool
	// stopped reports that a drop has stopped the machine.
	stopped bool
}

// timerless is the timers of a physical clock of a world. A machine measures each wait with a
// machine.Timer, and the store of a world runs in its manual mode, so no code of a world waits
// on the channel of a timer.
type timerless struct {
	// fault reports a fault to the case of the world. The world hashes a function of an effect
	// by whether it is nil, so the hash of an effect that contains a clock does not read the
	// case.
	fault func(err error)
}

// NewTimer reports errTimer as a fault of the case, and returns a timer of a clock that does not
// advance.
func (t timerless) NewTimer(d time.Duration) clock.Timer {
	t.fault(errTimer)
	return fake.New(sim.Start).NewTimer(d)
}

// runClock is the physical clock of the store of a world: the time of the run, which has no
// drift and which no fault of a clock moves. Its origin is sim.Start.
type runClock struct {
	timerless
	// w is the world of the run.
	w *world
}

var _ hlc.Physical = runClock{}

// Time returns the time of the run.
func (c runClock) Time() time.Time { return c.w.Now() }

// Elapsed returns the time of the run since sim.Start.
func (c runClock) Elapsed() time.Duration { return c.w.Now().Sub(sim.Start) }

// hostClock is the physical clock of the machines of a host: the clock of the host in the
// world. The jumps and the strobe of the clock move its wall time, and they do not move its
// monotonic time, which the timers of the host wait for.
type hostClock struct {
	timerless
	// clk is the clock of the host in the world.
	clk *sim.Clock
}

var _ hlc.Physical = hostClock{}

// Time returns the time of the clock of the host.
func (c hostClock) Time() time.Time { return c.clk.Now() }

// Elapsed returns the monotonic time of the host.
func (c hostClock) Elapsed() time.Duration { return c.clk.Monotonic() }

// system takes the inputs of a system from c: the preset, then the hosts, the namespaces, the
// slot count and the mode of a loss of the lease of each namespace, and the seed and the zone
// of each host. It returns the function that builds the world of those inputs.
//
//dokimi:system
func system(c *prop.Case) func(w *world) {
	return build(c, c.Draw(presets, "preset"), &variant{})
}

// build takes the hosts, the namespaces, the slot count and the mode of a loss of the lease of
// each namespace, and the seed and the zone of each host from c, apart from the inputs that v
// fixes. It returns the function that builds the world of the preset p over those inputs, with
// the part that v replaces: one store, one reference application, and each host with its
// machines.
func build(c *prop.Case, p config.PresetName, v *variant) func(w *world) {
	hostCounts, slotCounts := prop.Integer(1, maxHosts), prop.Integer(1, maxSlots)
	zoneOf := prop.SampledFrom(zoneEast, zoneWest)
	if v.crowd != "" {
		hostCounts, slotCounts, zoneOf = prop.Just(1), prop.Just(maxSlots), prop.Just(v.crowd)
	}
	namespaceCounts, onLosses := prop.Integer(1, maxNamespaces), modes
	if v.paired {
		namespaceCounts = prop.Just(maxNamespaces)
	}
	if v.stop {
		onLosses = prop.Just(ownership.Stop)
	}
	hosts := c.Draw(hostCounts, "hosts")
	count := c.Draw(namespaceCounts, "namespaces")
	specs := make([]config.Spec, 0, count)
	layouts := make(map[shard.Namespace]int, count)
	onLoss := make(map[shard.Namespace]ownership.OnLeaseLoss, count)
	for _, ns := range namespaces[:count] {
		slots := 0
		if p != builtin.LeaderElection {
			slots = c.Draw(slotCounts, "slots")
		}
		mode := c.Draw(onLosses, "onLoss")
		spec, err := v.spec(ns, p, slots, mode)
		assert.NoError(c, err, "the spec of each namespace must encode")
		specs = append(specs, spec)
		layouts[ns], onLoss[ns] = max(slots, 1), mode
	}
	edit, err := placementConfig(zoneRules, v.sources)
	assert.NoError(c, err, "the placement of a spec edit must encode")
	seeds, zones := make([]rand.Seed, hosts), make([]string, hosts)
	for i := range seeds {
		seeds[i] = rand.Seed(c.Draw(prop.Integer(0, math.MaxInt32), "seed"))
		zones[i] = c.Draw(zoneOf, "zone")
	}
	return func(w *world) {
		r := config.NewRegistry(builtin.Support())
		assert.NoError(c, builtin.Register(r), "the built-ins must register")
		leases := make(map[shard.Namespace]ownership.Lease, len(specs))
		roles := make(map[shard.Namespace][]replication.Role, len(specs))
		budgets := make(map[shard.Namespace]config.Budget, len(specs))
		partitioners := make(map[shard.Namespace]partition.Partitioner, len(specs))
		probes := make(map[shard.Namespace][][]byte, len(specs))
		for i := range specs {
			ns := specs[i].Namespace
			res, err := config.Resolve(r, &specs[i], provided)
			assert.NoError(c, err, "the spec of each namespace must resolve")
			own, err := config.Build[ownership.Config](r, res, config.Ownership)
			assert.NoError(c, err, "the ownership of each namespace must build")
			model, err := config.Build[*replication.Compiled](r, res, config.Replication)
			assert.NoError(c, err, "the state model of each namespace must build")
			part, err := config.Build[partition.Partitioner](r, res, config.Partitioning)
			assert.NoError(c, err, "the partitioner of each namespace must build")
			leases[ns], roles[ns], budgets[ns], partitioners[ns] = own.Lease, model.Model.Roles, res.Spec.Budget, part
			probes[ns] = probesOf(part, layouts[ns])
		}
		timers := timerless{fault: func(err error) { assert.Fault(c, err) }}
		st := memory.New(hlc.NewOver(storeNode, 0, runClock{timerless: timers, w: w}), memory.WithManualTime())
		s := &env{
			tb:           c,
			timers:       timers,
			store:        st,
			members:      st.Membership(),
			app:          NewFencedStore(),
			registry:     r,
			specs:        specs,
			edit:         edit,
			leases:       leases,
			roles:        roles,
			budgets:      budgets,
			partitioners: partitioners,
			probes:       probes,
			modes:        onLoss,
			layouts:      layouts,
			waits:        waits{joins: make(map[waitKey]*joining)},
			variant:      *v,
			forged:       make(map[*protocol.Request]bool),
			epochs: epochs{
				reserved:   make(map[shardKey]map[epoch.Epoch]bool),
				highest:    make(map[shardKey]epoch.Epoch),
				operations: make(map[shardKey]shard.OperationID),
			},
			fences:      fences{acquired: make(map[fenceKey]epoch.Epoch), recorded: make(map[tenure]bool)},
			assignments: assignments{scopes: make(map[scopeKey]*assignment)},
			admissions:  assignments{scopes: make(map[scopeKey]*assignment)},
			constraints: assignments{scopes: make(map[scopeKey]*assignment)},
			reporting:   reporting{passes: make(map[waitKey]*placement.Input)},
			labels:      make(map[nodeKey]tag.Tags),
			keeping:     keeping{handles: make(map[waitKey]protocol.Key), kept: make(map[keptKey]bool)},
			coverage:    coverage{cleanups: make(map[cleanupKey][]protocol.Key)},
			settling:    settling{commits: make(map[waitKey]store.CommitID), unknown: make(map[procKey]store.CommitID)},
			isolation:   isolation{put: make(map[scopeKey]map[shard.ID][]epoch.Epoch)},
			bounding: bounding{
				scopes:    assignments{scopes: make(map[scopeKey]*assignment)},
				callbacks: make(map[waitKey]callbackKey),
				inflight:  make(map[callbackKey]int),
			},
			ordering: ordering{
				sequences:   make(map[ackKey]uint64),
				generations: make(map[procKey]epoch.Epoch),
				routes:      make(map[routeKey]high),
			},
			outages:   outages{closing: make(map[procKey]bool)},
			deadlines: deadlines{joins: make(map[procKey]machine.Token)},
			counting:  counting{generations: make(map[procKey]epoch.Epoch)},
			sightings: sightings{last: make(map[string]*sighting)},
			catalog:   catalog{shards: make(map[shard.Namespace][]shard.ID)},
		}
		for i, seed := range seeds {
			s.add(w, hostPrefix+strconv.Itoa(i+1), seed, zones[i])
		}
	}
}

// placementConfig returns the configuration of the planner of a spec of a simulation: the
// constraint labels with rules, and a search with the maxSourceShards sources. It leaves the
// constraints out for no rules, and the search for sources of 0, so the spec keeps those of its
// preset.
//
// Error modes: the errors of encoding/json for the configuration.
func placementConfig(rules []constraint.Rule, sources int) (json.RawMessage, error) {
	type search struct {
		MaxSourceShards int `json:"maxSourceShards"`
	}
	var cfg struct {
		Search      *search             `json:"search,omitempty"`
		Constraints []planner.Selection `json:"constraints,omitempty"`
	}
	if len(rules) > 0 {
		labels, err := json.Marshal(constraint.LabelsConfig{Rules: rules})
		if err != nil {
			return nil, err
		}
		cfg.Constraints = []planner.Selection{{Name: constraint.LabelsName, Config: labels}}
	}
	if sources > 0 {
		cfg.Search = &search{MaxSourceShards: sources}
	}
	return json.Marshal(&cfg)
}

// ownershipOf returns the configuration of exclusive ownership of a namespace of a simulation:
// fenced, with a lease of the maxDriftRate driftRate and the mode onLoss of a loss of the lease.
//
// Error modes: the errors of encoding/json for the configuration.
func ownershipOf(onLoss ownership.OnLeaseLoss) (json.RawMessage, error) {
	type lease struct {
		MaxDriftRate float64               `json:"maxDriftRate"`
		OnLoss       ownership.OnLeaseLoss `json:"onLoss"`
	}
	return json.Marshal(struct {
		Lease  lease `json:"lease"`
		Fenced bool  `json:"fenced"`
	}{Lease: lease{MaxDriftRate: driftRate, OnLoss: onLoss}, Fenced: true})
}

// spec returns the first version of the spec of the namespace ns of a system of v: the preset p,
// exclusive ownership with the mode onLoss of a loss of the lease, and for every preset other than
// builtin.LeaderElection the partitioner fixed-slots with slots slots. Its planner has the
// constraint of the zones unless v is unconstrained, and the search of v. A tight v gives it the
// bounds of one.
//
// Error modes: the errors of encoding/json for the configurations of the planner and the
// ownership.
func (v *variant) spec(ns shard.Namespace, p config.PresetName, slots int, onLoss ownership.OnLeaseLoss) (
	config.Spec, error,
) {
	rules := zoneRules
	if v.unconstrained {
		rules = nil
	}
	place, err := placementConfig(rules, v.sources)
	if err != nil {
		return config.Spec{}, err
	}
	own, err := ownershipOf(onLoss)
	if err != nil {
		return config.Spec{}, err
	}
	b := configtest.NewSpecBuilder().
		WithNamespace(ns).
		WithPreset(p).
		WithPlacement(config.Selection{Config: place}).
		WithOwnership(config.Selection{Name: builtin.ExclusiveName, Config: own}).
		WithVersion(firstVersion)
	if p != builtin.LeaderElection {
		raw := `{"` + slotsKey + `":` + strconv.Itoa(slots) + `}`
		b = b.WithPartitioning(config.Selection{Name: fixedslots.Name, Config: json.RawMessage(raw)})
	}
	if v.tight {
		one := 1
		b = b.WithBudget(config.Budget{MaxTransitions: &one, MaxNodeTransitions: &one, NodeCallbacks: &one})
	}
	return b.Build(), nil
}

// probesOf returns one key of each of the n positions of the partitioner part: the first key of
// the form key-<k>, for k below maxKeys, at each position.
func probesOf(part partition.Partitioner, n int) [][]byte {
	var keys, positions [][]byte
	var key, pos []byte
	for k := 0; k < maxKeys && len(keys) < n; k++ {
		key = strconv.AppendInt(append(key[:0], keyPrefix...), int64(k), 10)
		pos = part.Position(pos[:0], key)
		if !slices.ContainsFunc(positions, func(p []byte) bool { return bytes.Equal(p, pos) }) {
			positions = append(positions, slices.Clone(pos))
			keys = append(keys, slices.Clone(key))
		}
	}
	return keys
}

// add adds the host name to w with a source of randomness from seed, whose node has the label
// of the zone zone. The host spawns the root, the agent of the root, the agent of the node and
// the router of each namespace.
func (s *env) add(w *world, name string, seed rand.Seed, zone string) {
	h := &host{
		sys:    s,
		rand:   seeded.New(seed),
		logger: slog.New(slog.DiscardHandler),
		parts:  make(map[shard.Namespace]*part, len(s.specs)),
		cells:  make(map[string]*hosted),
		name:   name,
		node: shard.Node{
			ID:       shard.NodeID(name),
			Address:  name + port,
			Identity: shard.Identity(name),
			Labels:   tag.Tags{{Key: zoneLabel, Value: zone}},
		},
		index: len(s.hosts) + 1,
	}
	s.hosts = append(s.hosts, h)
	wh := w.AddHost(name)
	for i := range s.specs {
		spec := &s.specs[i]
		h.parts[spec.Namespace] = &part{}
		for _, r := range spawned {
			a := address{host: name, namespace: spec.Namespace, role: r}
			wh.Spawn(a.String(), func(clk *sim.Clock) sim.Machine[machine.Event, machine.Effect] {
				return h.start(clk, a, spec)
			}, s, h, r)
		}
	}
}

// start returns a new machine of the role of a, for the namespace of spec, over the clock clk
// of the host in the world. The machines of an incarnation of the host read the time through
// one hybrid logical clock over clk, and report their instruments to one recorder. A start and
// a restart of the host start the root of the first namespace first, so that start builds the
// clock and the recorder of the incarnation, as a process of sharder builds them when it
// starts. start keeps the machine as the latest machine of its role in the part of the
// namespace, and fails the case at a config that the constructor of the machine refuses. It
// returns the machine of the variant of the env for an agent and for a router.
func (h *host) start(clk *sim.Clock, a address, spec *config.Spec) machine.Machine {
	if a.role == spawned[0] && a.namespace == h.sys.specs[0].Namespace {
		physical := hostClock{timerless: h.sys.timers, clk: clk}
		h.clock = hlc.NewOver(clock.NodeID(h.index), clock.DefaultMaxOffset, physical)
		h.reporter = telemetrytest.NewRecorder()
	}
	ns, operators, p := spec.Namespace, []shard.Identity{operator}, h.parts[spec.Namespace]
	var m machine.Machine
	var err error
	switch a.role {
	case roleRoot:
		p.root, err = root.New(&root.Config{
			Clock:       h.clock,
			Rand:        h.rand,
			Reporter:    h.reporter,
			Provided:    provided,
			Logger:      h.logger,
			Fits:        func(mu *store.Mutation) error { return h.sys.store.Fits(shard.Scope{Namespace: ns}, mu) },
			Registry:    h.sys.registry,
			Operators:   operators,
			Node:        h.node,
			Spec:        *spec,
			Parallelism: 1,
		})
		m = p.root
	case roleRootAgent:
		node := h.node
		node.Capacity = map[string]float64{root.Capacity: 1}
		p.rootAgent, err = agent.New(&agent.Config{
			Clock:     h.clock,
			Rand:      h.rand,
			Reporter:  h.reporter,
			Provided:  provided,
			Logger:    h.logger,
			Registry:  h.sys.registry,
			Namespace: ns,
			Node:      node,
			Root:      true,
		})
		m = p.rootAgent
	case roleAgent:
		p.agent, err = agent.New(&agent.Config{
			Clock:     h.clock,
			Rand:      h.rand,
			Reporter:  h.reporter,
			Provided:  provided,
			Logger:    h.logger,
			Registry:  h.sys.registry,
			Namespace: ns,
			Node:      h.node,
		})
		m = p.agent
	default:
		p.router, err = routes.New(&routes.Config{
			Rand:      h.rand,
			Reporter:  h.reporter,
			Logger:    h.logger,
			Registry:  h.sys.registry,
			Provided:  provided,
			Namespace: ns,
		})
		m = p.router
	}
	assert.NoError(h.sys.tb, err, "the config of each machine of a host must be valid")
	switch v := &h.sys.variant; {
	case a.role == roleAgent && v.agent != nil:
		return v.agent(h.sys, p.agent)
	case a.role == roleRootAgent && v.rootAgent != nil:
		return v.rootAgent(h.sys, p.rootAgent)
	case a.role == roleRouter && v.router != nil:
		return v.router(h.sys, p.router)
	}
	return m
}
