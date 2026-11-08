// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"errors"

	"go.thesmos.sh/core/errs"
)

// The sentinel errors of package machinetest.
var (
	// ErrEnded reports a write transaction of a tenure that DropShard or RevokeShard ended,
	// and a commit of a transaction that has ended. Begin and Txn.Commit of a FencedStore
	// return an error that wraps it. It classifies as errs.Invalid.
	ErrEnded = errs.WithClass(errors.New("machinetest: the tenure or the transaction has ended"), errs.Invalid)

	// ErrUnknownRecordKind reports text that is not the spelling of a kind of a record.
	// ParseRecordKind and RecordKind.UnmarshalText return an error that wraps it. It
	// classifies as errs.Invalid.
	ErrUnknownRecordKind = errs.WithClass(errors.New("machinetest: unknown kind of record"), errs.Invalid)

	// errReplica is the cause of each fault of a replica that the action fault reports to an
	// agent. It classifies as errs.Integrity, the class of a replica whose data failed.
	errReplica = errs.WithClass(errors.New("machinetest: the replica of the application failed"), errs.Integrity)

	// errTimer is the fault of a call of NewTimer on a physical clock of a world. A machine
	// measures each wait with a machine.Timer, and the store of a world runs in its manual mode,
	// so no code of a world waits on the channel of a timer. It classifies as errs.Unsupported,
	// the class of an implementation that is narrower than its contract.
	errTimer = errs.WithClass(errors.New("machinetest: a clock of a world has no timer of a channel"), errs.Unsupported)
)
