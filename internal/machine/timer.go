// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machine

import (
	"time"
)

// Timer is a wait of a duration on the clock of the process. The runtime arms a timer of
// the clock, and passes an [Elapsed] once the duration has passed. A timer of the
// production clock runs on the monotonic clock, and a timer of a fake clock runs on
// virtual time. A machine measures the deadline of a lease and every other deadline with a
// Timer. The wall time of the clock stops while the system clock steps back, so a deadline
// between two readings of the clock outlasts its duration by the size of the step. A Timer
// cannot be stopped, so the machine ignores an Elapsed that it no longer waits for.
type Timer struct {
	// Token is the token of the effect.
	Token Token
	// Duration is the duration of the wait. A wait of a Duration that is not above zero
	// ends at once.
	Duration time.Duration
}

var _ Effect = Timer{}

// effect marks Timer as an Effect.
func (Timer) effect() {}

// Elapsed is the end of the wait of a [Timer].
type Elapsed struct {
	// Token is the token of the Timer.
	Token Token
}

var _ Event = Elapsed{}

// event marks Elapsed as an Event.
func (Elapsed) event() {}
