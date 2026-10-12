// Copyright ThesmOS B.V. 2026
// SPDX-License-Identifier: BUSL-1.1

package builtin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"go.thesmos.sh/sharder/config"
	"go.thesmos.sh/sharder/shard"
)

// decodes is the number of times that Parse decodes a configuration. The second decode
// applies the configuration over the defaults that the fields of the first decode
// select.
const decodes = 2

// factory is the config.Factory of a built-in strategy. The strategy has the type T, and
// its configuration has the type C. P is *C, which implements config.Parsed, so Parse
// returns a *C.
//
// Parse decodes the raw configuration twice. Before each decode, defaults sets the
// defaults of the configuration from its fields, so a default can depend on another
// field, such as the renewal interval of a lease on its TTL. The second decode keeps
// every field that the raw configuration sets.
//
// # Concurrency
//
// A factory does not change after its constructor returns it. It is safe for concurrent
// use when the registry of its constructor is.
type factory[T, C any, P interface {
	*C
	config.Parsed
}] struct {
	// defaults sets the defaults of c from the fields of c. It is nil for a strategy
	// whose defaults are the zero C.
	defaults func(c P)
	// selected parses the strategies that c selects, and replaces their configurations
	// with the configurations that contain every default. It is nil for a strategy that
	// selects no strategy.
	selected func(c P) error
	// capabilities returns what the strategy provides and requires with c. It is nil for
	// a strategy without capabilities.
	capabilities func(c P) config.Capabilities
	// build builds the strategy from c.
	build func(c P) (T, error)
	// name is the registered name of the strategy.
	name shard.StrategyName
}

// Name returns the registered name of the strategy.
func (f *factory[T, C, P]) Name() shard.StrategyName {
	return f.name
}

// Parse returns the configuration that raw sets over the defaults of the strategy. An
// empty raw and the JSON null give the defaults.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of encoding/json for raw that does not
//     decode into the configuration, such as JSON with an unknown field.
//   - an error that wraps ErrConfig for data after the JSON value.
//   - an error that wraps ErrConfig for each strategy that the configuration selects and
//     the registry cannot parse.
func (f *factory[T, C, P]) Parse(raw json.RawMessage) (config.Parsed, error) {
	var c C
	for range decodes {
		if f.defaults != nil {
			f.defaults(&c)
		}
		if err := decode(raw, &c); err != nil {
			return nil, err
		}
	}
	if f.selected != nil {
		if err := f.selected(&c); err != nil {
			return nil, err
		}
	}
	return P(&c), nil
}

// Capabilities returns what the strategy provides and requires with cfg. It returns no
// capabilities for a configuration of another type than the one that Parse returns.
func (f *factory[T, C, P]) Capabilities(cfg config.Parsed) config.Capabilities {
	c, ok := cfg.(P)
	if !ok || f.capabilities == nil {
		return config.Capabilities{}
	}
	return f.capabilities(c)
}

// New builds the strategy from cfg.
//
// Error modes:
//   - an error that wraps ErrConfig for a configuration of another type than the one
//     that Parse returns.
//   - an error that wraps ErrConfig and the error of the constructor of the strategy.
func (f *factory[T, C, P]) New(cfg config.Parsed) (T, error) {
	c, ok := cfg.(P)
	if !ok {
		var zero T
		return zero, fmt.Errorf("%w: %s takes a %T, not a %T", ErrConfig, f.name, P(nil), cfg)
	}
	return f.build(c)
}

// empty is the configuration of the seven built-in strategies that have none. It
// encodes as {}, and Parse refuses every field.
type empty struct{}

// Validate returns nil, because every empty configuration is valid.
func (empty) Validate() error {
	return nil
}

// decode decodes the JSON configuration raw into c. Each field that raw sets replaces the
// field of c, and every other field keeps its value. decode leaves c unchanged for an
// empty raw.
//
// Error modes:
//   - an error that wraps ErrConfig and the error of encoding/json for raw that does not
//     decode into c, such as JSON with an unknown field.
//   - an error that wraps ErrConfig for data after the JSON value.
func decode(raw json.RawMessage, c any) error {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(c); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: data follows the JSON value", ErrConfig)
	}
	return nil
}

// built returns s, or the zero T and an error that wraps ErrConfig and err when err is
// not nil. The build functions of the factories return the results of the constructors
// of the strategy packages through it, so a constructor that fails never yields a T
// that contains a nil pointer.
func built[T any](s T, err error) (T, error) {
	if err != nil {
		var zero T
		return zero, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	return s, nil
}
