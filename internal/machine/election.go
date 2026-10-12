// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/shard"
)

// Campaign is a campaign for the election of a scope through store.Store.Campaign. The
// runtime passes the result as a [Campaigned]. It passes a [TermEnded] once a term that
// the election granted ends.
type Campaign struct {
	// Scope is the scope of the election.
	Scope shard.Scope
	// Token is the token of the effect.
	Token Token
}

var _ Effect = Campaign{}

// effect marks Campaign as an Effect.
func (Campaign) effect() {}

// Campaigned is the result of a [Campaign]. A Campaigned with an error is the last event
// of its campaign.
type Campaigned struct {
	// Err is the error of store.Store.Campaign, or nil.
	Err error
	// Token is the token of the Campaign.
	Token Token
	// Term is the fence epoch of the term that the election granted.
	Term epoch.Epoch
}

var _ Event = Campaigned{}

// event marks Campaigned as an Event.
func (Campaigned) event() {}

// TermEnded is the end of the term that the election granted to a [Campaign]. The runtime
// passes it once the channel of store.Leadership.Done is closed, such as after a
// [Resign]. It is the last event of its campaign.
type TermEnded struct {
	// Token is the token of the Campaign.
	Token Token
}

var _ Event = TermEnded{}

// event marks TermEnded as an Event.
func (TermEnded) event() {}

// Resign is the end of a [Campaign]. The runtime resigns a granted term through
// store.Leadership.Resign. It then cancels the context of the campaign, so a failed
// resignation still ends the term. The runtime passes a [TermEnded] for a term that the
// election granted, also when the election granted the term while the campaign stopped.
// For a campaign that stops without a term, it passes a [Campaigned] with an error.
// Resign has no result of its own.
type Resign struct {
	// Token is the token of the Campaign.
	Token Token
}

var _ Effect = Resign{}

// effect marks Resign as an Effect.
func (Resign) effect() {}
