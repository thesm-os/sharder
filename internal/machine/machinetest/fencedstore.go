// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package machinetest

import (
	"fmt"
	"slices"

	"go.thesmos.sh/core/epoch"

	"go.thesmos.sh/sharder/fence"
	"go.thesmos.sh/sharder/protocol"
	"go.thesmos.sh/sharder/shard"
)

// RecordKind is the kind of a record of a FencedStore.
//
// The zero RecordKind is no record.
//
// # Encoding
//
// [RecordKind.String] returns the spelling of a RecordKind, such as "committed", and
// [ParseRecordKind] returns the kind of a spelling.
//
//dokimi:enum text=comment
type RecordKind uint8

const (
	// Acquired is an acquisition that raised the watermark of a fence key.
	Acquired RecordKind = 1 // acquired
	// Started is the start of a tenure through AddShard.
	Started RecordKind = 2 // started
	// Ended is the end of a tenure that runs, through DropShard or RevokeShard.
	Ended RecordKind = 3 // ended
	// Committed is one write of a committed transaction.
	Committed RecordKind = 4 // committed
)

// Record is one fact that a FencedStore records: an acquisition, the start or the end of a
// tenure, or one write of a committed transaction.
type Record struct {
	// Key is the key of the write of a Committed record, and empty for any other record.
	Key string
	// Value is the value of the write of a Committed record, and empty for any other record.
	Value string
	// Token is the token of the acquisition, of the tenure, or of the tenure of the write.
	Token fence.Token
	// Kind is the kind of the record.
	Kind RecordKind
}

// tenureState is the state of a tenure in a FencedStore.
type tenureState uint8

// The states of a tenure. A tenure without a state has neither started nor ended.
const (
	// running is a tenure that AddShard started and nothing ended.
	running tenureState = 1
	// ended is a tenure that DropShard or RevokeShard ended. Its transactions do not begin
	// or commit.
	ended tenureState = 2
)

// fenceKey is the fence key of a token: its namespace incarnation and its shard.
type fenceKey struct {
	// incarnation is the namespace incarnation of the shard.
	incarnation shard.NamespaceIncarnation
	// shard is the shard.
	shard shard.ID
}

// tenure is one tenure of a fence key: the key and the epoch of its token.
type tenure struct {
	// key is the fence key of the tenure.
	key fenceKey
	// epoch is the epoch of the tenure.
	epoch epoch.Epoch
}

// fenceState is the fence of one fence key: its watermark, its open write transactions, and
// the acquisitions that wait for them.
type fenceState struct {
	// open lists the open write transactions of the key, in the order of their begins.
	open []*Txn
	// waiting lists the acquisitions that wait for open transactions, in the order of
	// their calls.
	waiting []*Acquisition
	// watermark is the highest epoch that an acquisition or a commit has set.
	watermark epoch.Epoch
}

// FencedStore is the storage of the reference application of the simulation of a
// namespace. It keeps a fence for each shard, the write transactions of the tenures, the
// keys of the callbacks with their effects, and a record of each acquisition, tenure and
// write.
//
// The fence of each shard follows the fence laws of core. A write transaction checks the
// fence of its shard when it begins, and its commit checks the fence again. A commit at an
// epoch above the watermark advances the watermark, and a token at epoch.Zero is unfenced.
// An acquisition raises the watermark to the epoch of its token once every write transaction
// that checked the fence before it has ended. A transaction of a tenure below an
// acquisition that waits does not begin.
//
// AddShard and DropShard are the handler of the application. Each records the key of its
// callback with its effect, so a callback that arrives again does not repeat its effect.
// RevokeShard ends the work of a tenure.
//
// The zero FencedStore is not usable. Construct one with NewFencedStore.
//
// # Concurrency
//
// A FencedStore is not safe for concurrent use. The simulation calls it on the goroutine of
// its case.
type FencedStore struct {
	// fences maps each fence key that a call named to its fence.
	fences map[fenceKey]*fenceState
	// tenures maps each tenure that started or ended to its state.
	tenures map[tenure]tenureState
	// effects maps the key of each callback to the effects that the handler applied for it.
	effects map[protocol.Key]int
	// calls maps the key of each callback to the calls of the handler with it.
	calls map[protocol.Key]int
	// records lists the records of the store in the order in which it recorded them.
	records []Record
}

// NewFencedStore returns an empty store.
func NewFencedStore() *FencedStore {
	return &FencedStore{
		fences:  make(map[fenceKey]*fenceState),
		tenures: make(map[tenure]tenureState),
		effects: make(map[protocol.Key]int),
		calls:   make(map[protocol.Key]int),
	}
}

// Acquire starts the acquisition of the fence key of t at t.Epoch. The acquisition ends at
// once for a token at epoch.Zero, which is unfenced, and for an epoch below the watermark.
// Otherwise it raises the watermark to t.Epoch, and records the acquisition, once every
// write transaction of the key that is open at the call has ended. Acquire does not keep t.
func (s *FencedStore) Acquire(t *fence.Token) *Acquisition {
	a := &Acquisition{token: *t}
	if t.Epoch == epoch.Zero {
		a.done = true
		return a
	}
	f := s.fence(fenceKey{incarnation: t.Incarnation, shard: t.Shard})
	if t.Epoch < f.watermark || len(f.open) == 0 {
		s.complete(f, a)
		return a
	}
	a.before = slices.Clone(f.open)
	f.waiting = append(f.waiting, a)
	return a
}

// Begin begins a write transaction of the tenure of t. The transaction checks the fence of
// the key of t, unless t is at epoch.Zero. Begin does not keep t.
//
// Error modes:
//   - an error that wraps ErrEnded for a tenure that DropShard or RevokeShard ended.
//   - an error that wraps epoch.ErrFenced when the watermark, or an acquisition that waits,
//     is above t.Epoch.
func (s *FencedStore) Begin(t *fence.Token) (*Txn, error) {
	k := fenceKey{incarnation: t.Incarnation, shard: t.Shard}
	if s.tenures[tenure{key: k, epoch: t.Epoch}] == ended {
		return nil, fmt.Errorf("machinetest: a transaction of shard %v at epoch %d: %w", t.Shard, t.Epoch, ErrEnded)
	}
	f := s.fence(k)
	if t.Epoch != epoch.Zero {
		barrier := f.watermark
		for _, a := range f.waiting {
			barrier = max(barrier, a.token.Epoch)
		}
		if t.Epoch < barrier {
			return nil, fmt.Errorf("machinetest: a transaction of shard %v at epoch %d below %d: %w",
				t.Shard, t.Epoch, barrier, epoch.ErrFenced)
		}
	}
	tx := &Txn{s: s, key: k, token: *t}
	f.open = append(f.open, tx)
	return tx, nil
}

// AddShard starts the tenure of t for the callback req, as the handler of the application
// does. A write transaction of the tenure records the key of req with the start of the
// tenure when it commits, so a callback that arrives again with the same key returns nil
// without a second effect. Each call counts as a call of the key. AddShard does not keep req
// or t.
//
// Error modes:
//   - the error of Begin and of Txn.Commit of the transaction of the tenure.
func (s *FencedStore) AddShard(req *protocol.Request, t *fence.Token) error {
	k := req.Key()
	s.calls[k]++
	if s.effects[k] > 0 {
		return nil
	}
	tx, err := s.Begin(t)
	if err != nil {
		return err
	}
	tx.callback = &k
	return tx.Commit()
}

// DropShard ends the tenure of t for the callback req, as the handler of the application
// does. It records the key of req with the end of the tenure, so a callback that arrives
// again with the same key does not repeat the effect. Each call counts as a call of the key.
// The end of a tenure ends its open transactions, as RevokeShard does. DropShard does not
// keep req or t.
func (s *FencedStore) DropShard(req *protocol.Request, t *fence.Token) {
	k := req.Key()
	s.calls[k]++
	if s.effects[k] > 0 {
		return
	}
	s.effects[k]++
	s.end(t)
}

// RevokeShard ends the work of the tenure of t: it ends the open transactions of the tenure,
// and a later transaction of the tenure does not begin. It records the end of a tenure that
// runs. RevokeShard does not keep t.
func (s *FencedStore) RevokeShard(t *fence.Token) {
	s.end(t)
}

// Records returns the records of the store in the order in which it recorded them. The
// slice shares memory with the store, so the caller does not modify it.
func (s *FencedStore) Records() []Record {
	return slices.Clip(s.records)
}

// Effects returns the number of the effects that the handler applied for the callback key
// k: 1 once a callback with the key has taken effect, and 0 before.
func (s *FencedStore) Effects(k *protocol.Key) int {
	return s.effects[*k]
}

// Calls returns the number of the calls of AddShard and DropShard with the callback key k.
func (s *FencedStore) Calls(k *protocol.Key) int {
	return s.calls[*k]
}

// fence returns the fence of the key k, and creates the fence of a new key.
func (s *FencedStore) fence(k fenceKey) *fenceState {
	f, ok := s.fences[k]
	if !ok {
		f = &fenceState{}
		s.fences[k] = f
	}
	return f
}

// end ends the tenure of t and its open transactions, and records the end of a tenure that
// runs.
func (s *FencedStore) end(t *fence.Token) {
	k := fenceKey{incarnation: t.Incarnation, shard: t.Shard}
	id := tenure{key: k, epoch: t.Epoch}
	if s.tenures[id] == running {
		s.records = append(s.records, Record{Token: *t, Kind: Ended})
	}
	s.tenures[id] = ended
	f := s.fence(k)
	for _, tx := range slices.Clone(f.open) {
		if tx.token.Epoch == t.Epoch {
			s.finish(f, tx)
		}
	}
}

// finish ends the transaction tx of the fence f, and ends each acquisition of f that waits
// for no other transaction, in the order of their calls.
func (s *FencedStore) finish(f *fenceState, tx *Txn) {
	tx.closed = true
	f.open = slices.DeleteFunc(f.open, func(o *Txn) bool { return o == tx })
	f.waiting = slices.DeleteFunc(f.waiting, func(a *Acquisition) bool {
		a.before = slices.DeleteFunc(a.before, func(o *Txn) bool { return o == tx })
		if len(a.before) > 0 {
			return false
		}
		s.complete(f, a)
		return true
	})
}

// complete ends the acquisition a of the fence f. It raises the watermark of f to the epoch
// of a and records the acquisition, unless the watermark is above that epoch.
func (s *FencedStore) complete(f *fenceState, a *Acquisition) {
	a.done = true
	if a.token.Epoch < f.watermark {
		a.err = fmt.Errorf("machinetest: the acquisition of shard %v at epoch %d below %d: %w",
			a.token.Shard, a.token.Epoch, f.watermark, epoch.ErrFenced)
		return
	}
	f.watermark = a.token.Epoch
	s.records = append(s.records, Record{Token: a.token, Kind: Acquired})
}

// Acquisition is one acquisition of the fence of a shard. FencedStore.Acquire returns it.
//
// # Concurrency
//
// An Acquisition is not safe for concurrent use, as its store is not.
type Acquisition struct {
	// err is the error of the acquisition once it has ended.
	err error
	// before lists the open write transactions that the acquisition waits for.
	before []*Txn
	// token is the token of the acquisition.
	token fence.Token
	// done reports that the acquisition has ended.
	done bool
}

// Result reports whether the acquisition has ended, and returns its error once it has.
//
// Error modes:
//   - an error that wraps epoch.ErrFenced when the watermark was above the epoch of the
//     token of the acquisition when it ended.
func (a *Acquisition) Result() (bool, error) {
	return a.done, a.err
}

// Txn is one write transaction of a tenure. FencedStore.Begin returns it.
//
// # Concurrency
//
// A Txn is not safe for concurrent use, as its store is not.
type Txn struct {
	// s is the store of the transaction.
	s *FencedStore
	// callback is the key of the callback of AddShard whose start of the tenure the
	// transaction applies, and nil for any other transaction.
	callback *protocol.Key
	// writes lists the writes of the transaction as Committed records, in the order of the
	// calls of Put.
	writes []Record
	// token is the token of the tenure of the transaction.
	token fence.Token
	// key is the fence key of the tenure.
	key fenceKey
	// closed reports a transaction that has ended.
	closed bool
}

// Put sets the value of key in the transaction.
func (tx *Txn) Put(key, value string) {
	tx.writes = append(tx.writes, Record{Key: key, Value: value, Token: tx.token, Kind: Committed})
}

// Commit commits the writes of the transaction, and records each write with the token of
// its tenure in the order of the calls of Put. A transaction of AddShard also records the
// key of its callback and the start of its tenure. A commit at an epoch above the watermark
// advances the watermark. The transaction then ends, and so does each acquisition that waits
// for it alone.
//
// Error modes:
//   - an error that wraps ErrEnded for a transaction that has ended, also through the end
//     of its tenure.
//   - an error that wraps epoch.ErrFenced when the watermark is above the epoch of the
//     tenure of a fenced transaction. The transaction then ends without a write.
func (tx *Txn) Commit() error {
	if tx.closed {
		return fmt.Errorf("machinetest: a commit of shard %v at epoch %d: %w", tx.token.Shard, tx.token.Epoch, ErrEnded)
	}
	f := tx.s.fences[tx.key]
	var err error
	if e := tx.token.Epoch; e != epoch.Zero && e < f.watermark {
		err = fmt.Errorf("machinetest: a commit of shard %v at epoch %d below %d: %w",
			tx.token.Shard, e, f.watermark, epoch.ErrFenced)
	} else {
		f.watermark = max(f.watermark, e)
		tx.s.records = append(tx.s.records, tx.writes...)
		if tx.callback != nil {
			tx.s.effects[*tx.callback]++
			tx.s.tenures[tenure{key: tx.key, epoch: e}] = running
			tx.s.records = append(tx.s.records, Record{Token: tx.token, Kind: Started})
		}
	}
	tx.s.finish(f, tx)
	return err
}

// Abort ends the transaction without a write, and so does each acquisition that waits for
// it alone. An Abort of a transaction that has ended does nothing.
func (tx *Txn) Abort() {
	if !tx.closed {
		tx.s.finish(tx.s.fences[tx.key], tx)
	}
}
