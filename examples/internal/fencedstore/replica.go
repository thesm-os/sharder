// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package fencedstore

import (
	"maps"
	"sync"
)

// Replica applies the committed transactions of its store in order, when it reads. A paused
// replica keeps its position until it resumes.
//
// The zero Replica is not usable. Construct one with Store.NewReplica.
//
// # Concurrency
//
// A Replica is safe for concurrent use.
type Replica struct {
	// s is the store of the replica.
	s *Store
	// values maps each key to the value at the position of the replica.
	values map[string]string
	// position is the number of the committed transactions that the replica has applied.
	position int
	// reads counts the reads of the replica.
	reads int
	// paused is true while the replica keeps its position.
	paused bool
	// mu guards the fields above.
	mu sync.Mutex
}

var _ Reader = (*Replica)(nil)

// Get returns the value of key at the position of r, and reports whether key has one. An r
// that is not paused first applies the transactions that its store has committed.
func (r *Replica) Get(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apply()
	r.reads++
	v, ok := r.values[key]
	return v, ok
}

// Position returns the number of the committed transactions that r has applied. An r that is
// not paused first applies the transactions that its store has committed.
func (r *Replica) Position() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apply()
	return r.position
}

// Pause keeps r at its position until Resume.
func (r *Replica) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paused = true
}

// Resume lets r apply the transactions that its store has committed.
func (r *Replica) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paused = false
}

// Reads returns the number of the calls of Get on r.
func (r *Replica) Reads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// apply applies the transactions that the store has committed after the position of r,
// unless r is paused. The caller has locked r.mu.
func (r *Replica) apply() {
	if r.paused {
		return
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, writes := range r.s.log[r.position:] {
		maps.Copy(r.values, writes)
	}
	r.position = len(r.s.log)
}
