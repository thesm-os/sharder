// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package memory

// Point is the name of a point at which FailAt interrupts a commit or a renewal of a
// lease.
type Point string

// The points at which FailAt interrupts a call.
const (
	// PointCommitApply is before a commit applies. The commit is lost, and Commit
	// returns an error that wraps version.ErrOutcomeUnknown.
	PointCommitApply Point = "commit/apply"
	// PointCommitAck is after a commit applies, before Commit returns. The commit and
	// its receipt remain, the reply is lost, and Commit returns an error that wraps
	// version.ErrOutcomeUnknown.
	PointCommitAck Point = "commit/ack"
	// PointRenewApply is before a renewal of a lease applies. The renewal is lost, so it
	// moves neither the expiry of the lease nor the deadline of its session.
	PointRenewApply Point = "renew/apply"
	// PointRenewAck is after a renewal of a lease applies. The store moves the expiry of
	// the lease, the reply is lost, and the deadline of the session does not move.
	PointRenewAck Point = "renew/ack"
)

// Points returns the points at which FailAt interrupts a call.
func (*Store) Points() []Point {
	return []Point{PointCommitApply, PointCommitAck, PointRenewApply, PointRenewAck}
}

// FailAt makes the next commit or renewal that passes p fail, as the documentation of p
// states. Each call arms one fault. FailAt of a Point that is not one of Points makes no
// call fail.
func (s *Store) FailAt(p Point) {
	s.lock()
	defer s.mu.Unlock()
	s.armed[p]++
}

// StartOutage makes every later call fail with an error that wraps ErrOutage until
// EndOutage, as during an outage of the backend. A commit during an outage does not
// apply, and a renewal during an outage fails. A watch ends with an error that wraps
// ErrOutage at its next event.
func (s *Store) StartOutage() {
	s.lock()
	defer s.mu.Unlock()
	s.outage = true
	for _, sc := range s.scopes {
		sc.notify()
	}
}

// EndOutage ends the outage that StartOutage began.
func (s *Store) EndOutage() {
	s.lock()
	defer s.mu.Unlock()
	s.outage = false
}

// Compact discards every generation of every scope below its current one, so a page
// of a Read or a watch from an older generation fails with an error that wraps
// store.ErrCompacted.
func (s *Store) Compact() {
	s.lock()
	defer s.mu.Unlock()
	for _, sc := range s.scopes {
		sc.compact()
	}
}
