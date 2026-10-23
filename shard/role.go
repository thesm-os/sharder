// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package shard

//go:generate go tool kanon -type=Role -validate=valid

import "fmt"

// Role is the role of a replica in a replica state model, such as [RolePrimary]. A
// model of package replication declares the roles that the replicas of a shard take,
// the most replicas in each role, the transitions between the roles, and the role
// that writes.
//
// The zero Role is [RoleNone], the role of no replica, such as the current role in a
// callback that does not change a role.
//
// # Encoding
//
// kanon encodes a Role as an unsigned varint of its number. A codec returns an error
// that wraps [ErrUnknownRole] for a Role outside the eight roles, on encode and on
// decode. [Role.String] returns the spelling of a Role, such as "primary", and the form
// "Role(n)" for any other value. [ParseRole] returns the Role of a spelling. The text
// methods write and read the spelling, so encoding/json writes a Role as a string. kanon
// checks for ValidateKanon before it checks for text methods, so a codec still writes a
// Role as its number.
//
// # Concurrency
//
// A Role is a comparable value.
//
// # Allocation contract
//
// [Role.IsValid], and [Role.String] of one of the eight roles, do not allocate. String
// allocates the form of any other value.
//
//dokimi:enum text=comment
type Role uint8

const (
	// RoleNone is the role of no replica.
	RoleNone Role = 0 // none
	// RoleOffline is the role of a replica that is assigned and not serving. A new
	// replica starts in it, and a replica passes through it before it is dropped.
	RoleOffline Role = 1 // offline
	// RoleDropped is the role of a replica that has left its node. Every model ends
	// in it, and only RoleOffline leads to it.
	RoleDropped Role = 2 // dropped
	// RolePrimary is the writer role of the models primary-only and
	// primary-secondary.
	RolePrimary Role = 3 // primary
	// RoleSecondary is the role of a replica that serves without writing: a
	// follower of the primary in primary-secondary, and every replica of
	// secondary-only.
	RoleSecondary Role = 4 // secondary
	// RoleLeader is the writer role of a consensus group.
	RoleLeader Role = 5 // leader
	// RoleFollower is the role of a voter of a consensus group that does not lead.
	RoleFollower Role = 6 // follower
	// RoleLearner is the role of a member of a consensus group that copies the state
	// of the shard and does not vote.
	RoleLearner Role = 7 // learner
)

// RoleCount is the number of roles, from RoleNone to RoleLearner. An array with one
// element per role has this length, and each Role that IsValid accepts indexes it.
const RoleCount = 8

// valid returns nil for one of the eight roles, and an error that wraps [ErrUnknownRole]
// and states the number of r for any other. The generated ValidateKanon calls it, so a
// codec refuses a role outside the eight.
func (r Role) valid() error {
	if !r.IsValid() {
		return fmt.Errorf("%w: %d", ErrUnknownRole, uint8(r))
	}
	return nil
}
