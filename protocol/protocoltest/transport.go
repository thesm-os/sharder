// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package protocoltest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.dokimi.dev/assert"
	"go.dokimi.dev/assert/prop"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/errs"
	"go.thesmos.sh/core/id"

	"go.thesmos.sh/sharder/budget"
	"go.thesmos.sh/sharder/proposal"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/protocol/local"
	"go.thesmos.sh/sharder/shard"
)

// The names in the messages of the suite.
const (
	// namespace is the namespace of the scope of the suite.
	namespace shard.Namespace = "protocoltest"
	// cell is the cell of the scope of the suite.
	cell shard.CellID = "cell-1"
	// node is the node of the agent of the suite.
	node shard.NodeID = "node-a"
	// feature is a feature that no release of the protocol defines.
	feature protocol.Feature = "protocoltest-unknown"
	// fakeAddress is the address of the controller of the verified fake of the planted
	// defects of Connector.
	fakeAddress = "protocoltest-controller:7000"
)

// The names of the methods of the clients of a transport.
const (
	methodDo      = "Do"
	methodReport  = "Report"
	methodSubmit  = "Submit"
	methodDisrupt = "Disrupt"
	methodDone    = "Done"
)

// labelMethod labels the method of the clients that a check calls.
const labelMethod = "method"

// The bounds of the generated messages.
const (
	// maxEpoch is the largest epoch, sequence and generation of a message.
	maxEpoch = 1 << 40
	// maxSeconds is the latest instant of a message, in seconds after the Unix epoch.
	maxSeconds = 1 << 40
	// maxBytes is the most bytes of a position, a body or a result.
	maxBytes = 16
	// maxList is the most elements of a list of a message.
	maxList = 3
)

// errServer is the error of the servers of the check of error classes.
var errServer = errors.New("protocoltest: the server failed")

// scope is the scope of the messages of the suite.
var scope = shard.Scope{Namespace: namespace, Cell: cell}

// The generators of the parts of the messages of the suite.
var (
	// identities generates the identity of a caller: none, a node, or an operator.
	identities = prop.SampledFrom[shard.Identity]("", "spiffe://protocoltest/node-a", "spiffe://protocoltest/operator")
	// epochs generates an epoch, a sequence or a generation.
	epochs = prop.Integer[epoch.Epoch](0, maxEpoch)
	// instants generates an instant in UTC, in whole seconds.
	instants = prop.Integer[int64](0, maxSeconds).Map(func(s int64) time.Time { return time.Unix(s, 0).UTC() })
	// ids generates an identifier of 16 bytes.
	ids = prop.Bytes(prop.MinSize(id.Size128), prop.MaxSize(id.Size128)).Map(func(b []byte) id.ID {
		return id.ID128(b).ID()
	})
	// shards generates a shard ID. A draw of 16 zero bytes is the zero ID.
	shards = prop.Bytes(prop.MinSize(id.Size128), prop.MaxSize(id.Size128)).Map(func(b []byte) shard.ID {
		return shard.ID{ID128: id.ID128(b)}
	})
	// payloads generates a position, a body or a result.
	payloads = prop.Bytes(prop.MaxSize(maxBytes))
	// roles generates each role of a replica.
	roles = prop.SampledFrom(shard.RoleNone, shard.RoleOffline, shard.RoleDropped, shard.RolePrimary,
		shard.RoleSecondary, shard.RoleLeader, shard.RoleFollower, shard.RoleLearner)
	// classes generates each class of core's errs.
	classes = prop.SampledFrom(errs.Unspecified, errs.Transient, errs.Conflict, errs.NotFound, errs.Invalid,
		errs.Unsupported, errs.Denied, errs.Integrity)
	// requests generates a callback to the agent of the suite.
	requests = prop.Composite(func(c *prop.Case) any {
		return &protocol.Request{
			Scope:           scope,
			Node:            node,
			Range:           shard.Range{Start: c.Draw(payloads, "start"), End: c.Draw(payloads, "end")},
			Operation:       c.Draw(operations, "operation"),
			Step:            c.Draw(prop.Integer(0, maxList), "step"),
			NodeIncarnation: c.Draw(epochs, "node incarnation"),
			Epoch:           c.Draw(epochs, "epoch"),
			ControllerEpoch: c.Draw(epochs, "controller epoch"),
			Shard:           c.Draw(shards, "shard"),
			Call:            c.Draw(prop.SampledFrom(protocol.Add, protocol.Drop), "call"),
			Path:            c.Draw(prop.SampledFrom(protocol.Forward, protocol.Abort, protocol.Cleanup), "path"),
			Role:            c.Draw(roles, "role"),
		}
	})
	// operations generates the transition of a callback.
	operations = prop.Composite(func(c *prop.Case) shard.OperationID {
		return shard.OperationID{
			Incarnation: shard.NamespaceIncarnation{ID: c.Draw(ids, "incarnation")},
			Shard:       c.Draw(shards, "shard"),
			Sequence:    uint64(c.Draw(epochs, "sequence")),
		}
	})
	// replicas generates what a node reports about one replica.
	replicas = prop.Composite(func(c *prop.Case) shard.Observed {
		return shard.Observed{
			Load: c.Draw(
				prop.Dict(prop.SampledFrom("cpu", "bytes"), prop.Float[float64](0, 1e6), prop.MaxSize(2)),
				"load",
			),
			Cause:    c.Draw(prop.String(prop.MaxSize(maxBytes)), "cause"),
			Reported: c.Draw(instants, "reported"),
			Range:    shard.Range{Start: c.Draw(payloads, "start"), End: c.Draw(payloads, "end")},
			Parents:  c.Draw(prop.List(shards, prop.MaxSize(maxList)), "parents"),
			Epoch:    c.Draw(epochs, "epoch"),
			Shard:    c.Draw(shards, "shard"),
			Role:     c.Draw(roles, "role"),
			Status: c.Draw(prop.SampledFrom(shard.StatusStarting, shard.StatusServing, shard.StatusStopping,
				shard.StatusFailed), "status"),
			Fault: c.Draw(prop.SampledFrom(shard.FaultNone, shard.FaultTransient, shard.FaultPermanent), "fault"),
		}
	})
	// reports generates a report of the node of the suite.
	reports = prop.Composite(func(c *prop.Case) any {
		return &protocol.Report{
			Scope:           scope,
			Node:            node,
			Replicas:        c.Draw(prop.List(replicas, prop.MaxSize(maxList)), "replicas"),
			NodeIncarnation: c.Draw(epochs, "node incarnation"),
			Sequence:        uint64(c.Draw(epochs, "sequence")),
			Incarnation:     shard.NamespaceIncarnation{ID: c.Draw(ids, "incarnation")},
		}
	})
	// proposals generates a proposal for the scope of the suite.
	proposals = prop.Composite(func(c *prop.Case) any {
		return &proposal.Proposal{
			Scope:       scope,
			Expires:     c.Draw(instants, "expires"),
			Body:        c.Draw(payloads, "body"),
			Base:        c.Draw(epochs, "base"),
			Incarnation: shard.NamespaceIncarnation{ID: c.Draw(ids, "incarnation")},
			ID:          proposal.ID{ID: c.Draw(ids, "id")},
			Kind: c.Draw(prop.SampledFrom(proposal.SpecChange, proposal.Move, proposal.Drain, proposal.Undrain,
				proposal.Explain), "kind"),
		}
	})
	// disruptions generates a disruption of nodes of the scope of the suite.
	disruptions = prop.Composite(func(c *prop.Case) any {
		start := c.Draw(instants, "start")
		return &budget.Disruption{
			Start: start,
			End:   start.Add(time.Hour),
			Scope: scope,
			Nodes: c.Draw(
				prop.List(prop.SampledFrom[shard.NodeID](node, "node-b"), prop.MaxSize(maxList)),
				"nodes",
			),
			NodeIncarnation: c.Draw(epochs, "node incarnation"),
			ID:              budget.ID{ID: c.Draw(ids, "id")},
			Incarnation:     shard.NamespaceIncarnation{ID: c.Draw(ids, "incarnation")},
			Impact: c.Draw(prop.SampledFrom(budget.NetworkLoss, budget.ProcessRestart, budget.FullLoss),
				"impact"),
			Negotiable: c.Draw(prop.Boolean(), "negotiable"),
		}
	})
	// ends generates the end of a reservation of the scope of the suite.
	ends = prop.Composite(func(c *prop.Case) any {
		return &protocol.DoneArgs{
			Scope:       scope,
			Incarnation: shard.NamespaceIncarnation{ID: c.Draw(ids, "incarnation")},
			ID:          budget.ID{ID: c.Draw(ids, "id")},
		}
	})
)

// Connector connects the clients of a transport to the servers of a check. The suite of
// a transport is the suite of a Connector over the transport, so each check chooses the
// metadata of the calls before the transport connects. Each check connects clients and
// servers of its own.
//
// # Concurrency
//
// A Connector is safe for concurrent use.
//
//dokimi:stub
//dokimi:suite
type Connector interface {
	// Connect returns the agent client and the controller client of a process whose calls
	// have the metadata md. The transport passes each callback for e.Node in the namespace
	// of e.Scope to e.Agent, and each call of the controller of e.Scope to e.Controller.
	// Connect can set the address of e.Node, and the address and the identity of the
	// controller of e.Cell.
	Connect(md protocol.Metadata, e *Endpoints) (protocol.AgentClient, protocol.ControllerClient)
}

// Endpoints are the servers of one case of the suite, and the node and cell records that a
// client uses to call them.
type Endpoints struct {
	// Agent is the agent of Node in the namespace of Scope.
	Agent protocol.AgentServer
	// Controller is the controller of Scope.
	Controller protocol.ControllerServer
	// Scope is the scope of the messages of the suite.
	Scope shard.Scope
	// Node is the node of Agent. Connect sets its Address when its transport dials one.
	Node shard.Node
	// Cell is the record of Controller. Connect sets its Controller and its
	// ControllerIdentity when its transport dials and authenticates the controller.
	Cell shard.Cell
}

// received is one call that the servers of a case received.
type received struct {
	// message is the message of the call.
	message any
	// method is the name of the method that the client called.
	method string
	// md is the metadata of the call.
	md protocol.Metadata
}

// servers is the agent and the controller of one case of the suite. They record each call
// that they receive, and return the results and the error of the case. They do not check
// the metadata of a call, so a check observes each call that the transport passes.
//
// # Concurrency
//
// A servers is safe for concurrent use.
type servers struct {
	// err is the error of every call.
	err error
	// received lists the calls that the servers received, in order.
	received []received
	// decision is the result of Disrupt.
	decision budget.Decision
	// receipt is the result of Submit.
	receipt protocol.Receipt
	// ack is the result of Report.
	ack protocol.Ack
	mu  sync.Mutex
}

// Do records r, and returns the error of the case.
func (s *servers) Do(_ context.Context, md protocol.Metadata, r *protocol.Request) error {
	s.record(methodDo, md, r)
	return s.err
}

// Report records r, and returns the ack and the error of the case.
func (s *servers) Report(_ context.Context, md protocol.Metadata, r *protocol.Report) (protocol.Ack, error) {
	s.record(methodReport, md, r)
	return s.ack, s.err
}

// Submit records p, and returns the receipt and the error of the case.
func (s *servers) Submit(_ context.Context, md protocol.Metadata, p *proposal.Proposal) (protocol.Receipt, error) {
	s.record(methodSubmit, md, p)
	return s.receipt, s.err
}

// Disrupt records d, and returns the decision and the error of the case.
func (s *servers) Disrupt(_ context.Context, md protocol.Metadata, d *budget.Disruption) (budget.Decision, error) {
	s.record(methodDisrupt, md, d)
	return s.decision, s.err
}

// Done records a, and returns the error of the case.
func (s *servers) Done(_ context.Context, md protocol.Metadata, a *protocol.DoneArgs) error {
	s.record(methodDone, md, a)
	return s.err
}

// record records one call.
func (s *servers) record(method string, md protocol.Metadata, message any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = append(s.received, received{message: message, method: method, md: md})
}

// calls returns the calls that the servers received, in order.
func (s *servers) calls() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.received)
}

// clients are the clients that Connect of a subject returns.
type clients struct {
	agent      protocol.AgentClient
	controller protocol.ControllerClient
}

// method is one method of the clients of a transport, as the checks of the suite call it.
type method struct {
	// messages generates the message of a call.
	messages prop.Generator[any]
	// call calls the method with message through cl, and returns its result.
	call func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error)
	// result returns the result that s returns for a call of the method.
	result func(s *servers) any
	// name is the name of the method.
	name string
}

// methods lists the methods of the clients of a transport.
var methods = []method{
	{
		name:     methodDo,
		messages: requests,
		call: func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error) {
			r, _ := message.(*protocol.Request)
			return returned[any](methodDo, nil, cl.agent.Do(ctx, &e.Node, r))
		},
		result: func(*servers) any { return nil },
	},
	{
		name:     methodReport,
		messages: reports,
		call: func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error) {
			r, _ := message.(*protocol.Report)
			ack, err := cl.controller.Report(ctx, &e.Cell, r)
			return returned(methodReport, ack, err)
		},
		result: func(s *servers) any { return s.ack },
	},
	{
		name:     methodSubmit,
		messages: proposals,
		call: func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error) {
			p, _ := message.(*proposal.Proposal)
			receipt, err := cl.controller.Submit(ctx, &e.Cell, p)
			return returned(methodSubmit, receipt, err)
		},
		result: func(s *servers) any { return s.receipt },
	},
	{
		name:     methodDisrupt,
		messages: disruptions,
		call: func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error) {
			d, _ := message.(*budget.Disruption)
			decision, err := cl.controller.Disrupt(ctx, &e.Cell, d)
			return returned(methodDisrupt, decision, err)
		},
		result: func(s *servers) any { return s.decision },
	},
	{
		name:     methodDone,
		messages: ends,
		call: func(ctx context.Context, cl clients, e *Endpoints, message any) (any, error) {
			a, _ := message.(*protocol.DoneArgs)
			return returned[any](methodDone, nil, cl.controller.Done(ctx, &e.Cell, a))
		},
		result: func(*servers) any { return nil },
	},
}

// localConnector connects a client of a local network to the servers of a check. It is
// the verified fake of the planted defects of Connector.
type localConnector struct{}

// Connect returns the client of a process with the metadata md on a local network with
// the agent and the controller of e. It sets the address of the controller of e.Cell to
// fakeAddress.
func (localConnector) Connect(md protocol.Metadata, e *Endpoints) (protocol.AgentClient, protocol.ControllerClient) {
	n := local.NewNetwork()
	n.AddAgent(e.Scope.Namespace, e.Node.ID, e.Agent)
	e.Cell.Controller = fakeAddress
	n.AddController(fakeAddress, e.Controller)
	l := n.Local(md)
	return l, l
}

// ClientsPassMessages checks that a client passes each message with the metadata of the
// caller to the server, and returns the result of the server.
//
//dokimi:check Connector.Connect defect=stripsCallers
func ClientsPassMessages(h *ConnectorHandle, c *prop.Case) {
	m := c.Draw(prop.SampledFrom(methods...), labelMethod)
	md := protocol.Metadata{Caller: c.Draw(identities, "caller"), Version: protocol.Current}
	s, e, cl := open(h, c, md)
	message := c.Draw(m.messages, "message")
	got, err := m.call(h.Context(), cl, e, message)
	assert.NoError(h, err, h.Statement())
	assert.Equal(h, got, m.result(s), h.Statement(), assert.EquateEmpty())
	calls := s.calls()
	assert.Length(h, calls, 1, h.Statement())
	assert.Equal(h, calls[0].method, m.name, h.Statement())
	assert.Equal(h, calls[0].md, md, h.Statement(), assert.EquateEmpty())
	assert.Equal(h, calls[0].message, message, h.Statement(), assert.EquateEmpty())
}

// stripsCallers returns a planted defect over the local transport whose clients send the
// metadata of the caller without its Caller.
func stripsCallers(h *ConnectorHandle) Connector {
	return metadataDefect(h, func(md *protocol.Metadata) { md.Caller = "" })
}

// ClientsKeepErrorClasses checks that a client returns an error of the class of the error
// of the server.
//
//dokimi:check Connector.Connect defect=declassifiesErrors
func ClientsKeepErrorClasses(h *ConnectorHandle, c *prop.Case) {
	m := c.Draw(prop.SampledFrom(methods...), labelMethod)
	s, e, cl := open(h, c, protocol.Metadata{Version: protocol.Current})
	class := c.Draw(classes, "class")
	s.err = errs.WithClass(errServer, class)
	_, err := m.call(h.Context(), cl, e, c.Draw(m.messages, "message"))
	assert.HasError(h, err, h.Statement())
	assert.Equal(h, errs.Classify(err), class, h.Statement())
}

// declassifiesErrors returns a planted defect over the local transport whose clients
// return the text of an error of the server without its class.
func declassifiesErrors(h *ConnectorHandle) Connector {
	return NewConnectorStub(h, ConnectorStubDelegateTo(localConnector{}), WithConnectorConnect(
		func(md protocol.Metadata, e *Endpoints) (protocol.AgentClient, protocol.ControllerClient) {
			agent, controller := localConnector{}.Connect(md, e)
			return declassifiedAgent(h, agent), declassifiedController(h, controller)
		}))
}

// declassifiedAgent returns the double of agent whose Do returns the text of an error of
// the server without its class.
func declassifiedAgent(h *ConnectorHandle, agent protocol.AgentClient) protocol.AgentClient {
	return NewAgentClientStub(h, AgentClientStubDelegateTo(agent), WithAgentClientDo(
		func(ctx context.Context, n *shard.Node, r *protocol.Request) error {
			return declassify(agent.Do(ctx, n, r))
		}))
}

// declassifiedController returns the double of controller whose methods return the text
// of an error of the server without its class.
func declassifiedController(h *ConnectorHandle, controller protocol.ControllerClient) protocol.ControllerClient {
	return NewControllerClientStub(h, ControllerClientStubDelegateTo(controller),
		WithControllerClientReport(
			func(ctx context.Context, c *shard.Cell, r *protocol.Report) (protocol.Ack, error) {
				ack, err := controller.Report(ctx, c, r)
				return ack, declassify(err)
			}),
		WithControllerClientSubmit(
			func(ctx context.Context, c *shard.Cell, p *proposal.Proposal) (protocol.Receipt, error) {
				receipt, err := controller.Submit(ctx, c, p)
				return receipt, declassify(err)
			}),
		WithControllerClientDisrupt(
			func(ctx context.Context, c *shard.Cell, d *budget.Disruption) (budget.Decision, error) {
				decision, err := controller.Disrupt(ctx, c, d)
				return decision, declassify(err)
			}),
		WithControllerClientDone(
			func(ctx context.Context, c *shard.Cell, a *protocol.DoneArgs) error {
				return declassify(controller.Done(ctx, c, a))
			}))
}

// ClientsRefuseOtherVersions checks that a client refuses a call of another major version
// before the server receives it.
//
//dokimi:check Connector.Connect defect=passesOtherVersions
func ClientsRefuseOtherVersions(h *ConnectorHandle, c *prop.Case) {
	m := c.Draw(prop.SampledFrom(methods...), labelMethod)
	md := protocol.Metadata{Version: c.Draw(prop.Integer(protocol.Current+1, protocol.Current+maxList), "version")}
	s, e, cl := open(h, c, md)
	_, err := m.call(h.Context(), cl, e, c.Draw(m.messages, "message"))
	assert.Equal(h, errs.Classify(err), errs.Unsupported, h.Statement())
	assert.ErrorIs(h, err, errors.ErrUnsupported, h.Statement())
	assert.Empty(h, s.calls(), h.Statement())
}

// passesOtherVersions returns a planted defect over the local transport whose clients
// send the metadata of the caller at the current major version.
func passesOtherVersions(h *ConnectorHandle) Connector {
	return metadataDefect(h, func(md *protocol.Metadata) { md.Version = protocol.Current })
}

// ClientsRefuseFeatures checks that a client refuses a call that requires a feature before
// the server receives it.
//
//dokimi:check Connector.Connect defect=dropsFeatures
func ClientsRefuseFeatures(h *ConnectorHandle, c *prop.Case) {
	m := c.Draw(prop.SampledFrom(methods...), labelMethod)
	md := protocol.Metadata{Version: protocol.Current, Requires: []protocol.Feature{feature}}
	s, e, cl := open(h, c, md)
	_, err := m.call(h.Context(), cl, e, c.Draw(m.messages, "message"))
	assert.Equal(h, errs.Classify(err), errs.Unsupported, h.Statement())
	assert.ErrorIs(h, err, errors.ErrUnsupported, h.Statement())
	assert.Empty(h, s.calls(), h.Statement())
}

// dropsFeatures returns a planted defect over the local transport whose clients send the
// metadata of the caller without the features that it requires.
func dropsFeatures(h *ConnectorHandle) Connector {
	return metadataDefect(h, func(md *protocol.Metadata) { md.Requires = nil })
}

// metadataDefect returns a planted defect over the local transport whose clients send the
// metadata of the caller after change.
func metadataDefect(h *ConnectorHandle, change func(md *protocol.Metadata)) Connector {
	return NewConnectorStub(h, ConnectorStubDelegateTo(localConnector{}), WithConnectorConnect(
		func(md protocol.Metadata, e *Endpoints) (protocol.AgentClient, protocol.ControllerClient) {
			change(&md)
			return localConnector{}.Connect(md, e)
		}))
}

// open returns the servers of one case with generated results, the endpoints of the
// servers, and the clients that the subject of h connects for the metadata md.
func open(h *ConnectorHandle, c *prop.Case, md protocol.Metadata) (*servers, *Endpoints, clients) {
	s := &servers{
		ack: protocol.Ack{
			Sequence:        uint64(c.Draw(epochs, "acked sequence")),
			Generation:      c.Draw(epochs, "generation"),
			ControllerEpoch: c.Draw(epochs, "controller epoch"),
		},
		receipt: protocol.Receipt{
			Result:      c.Draw(payloads, "result"),
			Generation:  c.Draw(epochs, "applied generation"),
			Incarnation: shard.NamespaceIncarnation{ID: c.Draw(ids, "applied incarnation")},
			Applied:     c.Draw(prop.Boolean(), "applied"),
		},
		decision: budget.Decision{
			Reason:     c.Draw(prop.String(prop.MaxSize(maxBytes)), "reason"),
			RetryAfter: c.Draw(prop.Duration(0, time.Hour), "retry after"),
			Allowed:    c.Draw(prop.Boolean(), "allowed"),
		},
	}
	e := &Endpoints{
		Agent:      s,
		Controller: s,
		Node:       shard.Node{ID: node, Cell: cell},
		Cell:       shard.Cell{ID: cell},
		Scope:      scope,
	}
	agent, controller := h.Subject().Connect(md, e)
	return s, e, clients{agent: agent, controller: controller}
}

// declassify returns an error with the text of err and without its class when err wraps
// errServer, and err otherwise.
func declassify(err error) error {
	if errors.Is(err, errServer) {
		return errors.New(err.Error())
	}
	return err
}

// returned returns the result and the error of a call of the method of a client, with the
// error wrapped in the name of the method.
func returned[T any](method string, result T, err error) (any, error) {
	if err != nil {
		return result, fmt.Errorf("protocoltest: %s: %w", method, err)
	}
	return result, nil
}
