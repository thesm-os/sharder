// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.thesmos.sh/core/clock"
	"go.thesmos.sh/core/epoch"
	"go.thesmos.sh/core/id/ulid"
	"go.thesmos.sh/core/page"
	"go.thesmos.sh/core/rand"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/membership"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
	"go.thesmos.sh/sharder/store"
)

// stateLimit is the limit of the page of a read of the state of a scope alone. The store
// returns at most one placement on that page, and the client does not read it.
const stateLimit = 1

// incomparable is the first field of each request and each result of the package. The
// compiler then refuses == on the type, so a field that the type gains later does not break
// a comparison. As the first field it does not change the size of the type.
type incomparable [0]func()

// Config is the configuration of a client. [New] refuses a Config without one of its
// required fields.
type Config struct {
	// Store reads the root scope of each namespace and the scope of each cell. It is
	// required.
	Store store.Reader
	// Membership watches the views of each cell. It is required.
	Membership membership.Membership
	// Transport sends the proposals and the disruptions to the controllers. It is required.
	Transport protocol.ControllerClient
	// Clock reads the time from which each proposal expires, and the time of each new ID. It
	// is required.
	Clock clock.Clock
	// Rand is the source of the random bits of each new ID. It is required.
	Rand rand.Rand
	// Lifetime is the time after which a proposal expires. It is required and positive. A
	// controller refuses a proposal whose Expires has passed, and a scope forgets the ID of an
	// applied proposal once its Expires has passed.
	Lifetime time.Duration
}

// Client runs the maintenance of the namespaces of one store: the status of a namespace, the
// proposals to its controllers and the disruptions of the budgets of its cells. Each call
// finds its controller in the records of the store, and fills in the base that the controller
// checks. A Client does not retry a call.
//
// The zero Client is not usable. Construct one with [New].
//
// # Concurrency
//
// A Client is safe for concurrent use when the clock and the source of randomness of its
// Config are, such as core's rand/crypto and rand/seeded.
type Client struct {
	// store reads the scopes.
	store store.Reader
	// membership watches the views.
	membership membership.Membership
	// transport calls the controllers.
	transport protocol.ControllerClient
	// clock reads the time from which each proposal expires.
	clock clock.Clock
	// ids generates the ID of each proposal and each disruption without one.
	ids *ulid.Generator
	// lifetime is the time after which a proposal expires.
	lifetime time.Duration
}

// New returns a client of cfg. It does not perform I/O.
//
// Error modes:
//   - an error that wraps ErrConfig for a nil cfg, for a cfg without a store, a membership,
//     a transport, a clock or a source of randomness, and for a lifetime that is not
//     positive.
func New(cfg *Config) (*Client, error) {
	switch {
	case cfg == nil:
		return nil, fmt.Errorf("%w: no configuration", ErrConfig)
	case cfg.Store == nil:
		return nil, fmt.Errorf("%w: no store", ErrConfig)
	case cfg.Membership == nil:
		return nil, fmt.Errorf("%w: no membership", ErrConfig)
	case cfg.Transport == nil:
		return nil, fmt.Errorf("%w: no transport", ErrConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: no clock", ErrConfig)
	case cfg.Rand == nil:
		return nil, fmt.Errorf("%w: no source of randomness", ErrConfig)
	case cfg.Lifetime <= 0:
		return nil, fmt.Errorf("%w: the lifetime %v is not positive", ErrConfig, cfg.Lifetime)
	}
	return &Client{
		store:      cfg.Store,
		membership: cfg.Membership,
		transport:  cfg.Transport,
		clock:      cfg.Clock,
		ids:        ulid.New(cfg.Clock, cfg.Rand),
		lifetime:   cfg.Lifetime,
	}, nil
}

// root reads the root scope of the namespace ns and its cells. The cells are the placements of
// the root scope, in position order.
//
// Error modes:
//   - the error of read.
//   - an error that wraps ErrNoNamespace for a root scope without a namespace incarnation.
func (c *Client) root(ctx context.Context, ns shard.Namespace) (store.State, []shard.Placement, error) {
	st, cells, err := c.read(ctx, shard.Scope{Namespace: ns}, shard.NamespaceIncarnation{})
	if err != nil {
		return store.State{}, nil, err
	}
	if st.Head.Incarnation.IsZero() {
		return store.State{}, nil, fmt.Errorf("%w: namespace %s", ErrNoNamespace, ns)
	}
	return st, cells, nil
}

// read reads the state of the scope s and every page of its placements, in position order.
// inc is the namespace incarnation that the client expects, or zero. Every page is at the
// generation of the state.
//
// Error modes:
//   - an error that wraps the error of a Read, of a page or of the close of a cursor.
func (c *Client) read(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) (
	store.State, []shard.Placement, error,
) {
	var placements []shard.Placement
	st, cur, err := c.store.Read(ctx, s, inc, store.Query{}, page.Page{})
	for err == nil {
		for p, perr := range cur.Seq(ctx) {
			if perr != nil {
				err = perr
				break
			}
			placements = append(placements, p)
		}
		next := cur.NextPage()
		err = errors.Join(err, cur.Close())
		if err != nil || next == "" {
			break
		}
		_, cur, err = c.store.Read(ctx, s, inc, store.Query{}, page.Page{Token: next})
	}
	if err != nil {
		return store.State{}, nil, fmt.Errorf("maintenance: read scope %v: %w", s, err)
	}
	return st, placements, nil
}

// state reads the state of the scope s without its placements. inc is the namespace
// incarnation that the client expects, or zero.
//
// Error modes:
//   - an error that wraps the error of the Read or of the close of its cursor.
func (c *Client) state(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) (store.State, error) {
	st, cur, err := c.store.Read(ctx, s, inc, store.Query{}, page.Page{Limit: stateLimit})
	if err == nil {
		err = cur.Close()
	}
	if err != nil {
		return store.State{}, fmt.Errorf("maintenance: read scope %v: %w", s, err)
	}
	return st, nil
}

// view returns the current view of the scope s in the namespace incarnation inc, the first
// view of a watch. It ends the watch after that view.
//
// Error modes:
//   - an error that wraps the error of the watch.
//   - an error for a watch that ends before it yields a view.
func (c *Client) view(ctx context.Context, s shard.Scope, inc shard.NamespaceIncarnation) (membership.View, error) {
	for v, err := range c.membership.Watch(ctx, s, inc) {
		if err != nil {
			return membership.View{}, fmt.Errorf("maintenance: the views of scope %v: %w", s, err)
		}
		return v, nil
	}
	return membership.View{}, fmt.Errorf(
		"maintenance: the watch of the views of scope %v ended before the current view",
		s,
	)
}

// accepted returns the version of the accepted spec of the namespace ns, whose root scope has
// the state root.
//
// Error modes:
//   - an error that wraps the error of encoding/json for a spec that does not decode.
func accepted(ns shard.Namespace, root *store.State) (epoch.Epoch, error) {
	var spec config.Spec
	if err := json.Unmarshal(root.Spec, &spec); err != nil {
		return epoch.Zero, fmt.Errorf("maintenance: the accepted spec of namespace %s: %w", ns, err)
	}
	return spec.Version, nil
}

// recordOf returns the record of the controller of the scope s, whose state is st.
//
// Error modes:
//   - an error that wraps ErrNoController for a record without the address of a controller.
func recordOf(st *store.State, s shard.Scope) (*shard.Cell, error) {
	if st.Cell.Controller == "" {
		return nil, fmt.Errorf("%w: scope %v", ErrNoController, s)
	}
	return &st.Cell, nil
}
