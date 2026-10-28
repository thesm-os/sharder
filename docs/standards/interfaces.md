<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# Interfaces

These rules cover the form of the calls that cross a package boundary,
and the way that form grows without breaking a caller or an
implementation. The last section measures the time of each form.

## Scope

The rules apply to these calls:

- Every method of an exported interface, such as the parts of the
  contract's `Implementation`, `Registry`, the log service's `Provider`,
  `Log` and `Writer`, and the engine's `Engine`.
- Every exported method of the contract's concrete types that a host
  calls, such as `Ledger.Read`.

They do not apply to these calls:

- A constructor. It takes a pointer to its configuration, as
  `node.Open(ctx, *node.Config)` does.
- A lifecycle method, such as `Run` and `Close`.
- An accessor that does not perform I/O and cannot fail, such as
  `Registry.Ledger` and `Entry.Head`.
- A builder or a setter of a concrete type, such as `Batch.Add`. A
  builder gains an input as a new method, and a new method of a concrete
  type does not break a caller.
- The methods of an unexported interface. Its callers and its
  implementations change in one package.
- A method that implements an interface of the standard library, such as
  `Read`, `WriteTo` and `Close` of a `PayloadReader`. It keeps the
  signature of that interface, so a host passes the value to functions
  such as `io.Copy`.

## Requests and results

- A call takes a context and one request. It returns one result and an
  error:

  ```go
  Read(ctx context.Context, req *ReadRequest) (ReadResult, error)
  ```

- The request contains every input of the call, including the identifier
  of its target, such as the ledger ID. The result contains every output.
- Each call has a request type and a result type of its own, named after
  the call: `ReadRequest` and `ReadResult`. Each type belongs to one
  call, so a field that one call gains does not change another call.
- A request type that the contract hands out from a pool keeps the name
  of its concept. `Batch` is the request of `Append`.
- The caller passes the request by pointer. The callee does not keep the
  request after it returns, and it returns the result by value.
- A call writes bulk data into the buffers that its request names, such
  as `Dst` and `Buf`. Its result then contains small values: a count, a
  head or a position.
- The network API's `.proto` message of a call has the fields of the
  call's request. Its `<Call>Response` message has the fields of the
  call's result.

## Growth without a break

- A new input or output of a call is a new field of its request or its
  result. The zero value of the field keeps the behaviour that the call
  had before the field existed.
- A field keeps its name, its type and its meaning once a release
  contains it.
- The call checks each required field. A missing one returns an error
  classified as `errs.Invalid`, because the compiler does not check it.
- A new required input breaks every caller, because the callers that
  exist do not set it. It needs a new call or a new major version.
- A released interface never gains a method. A new call goes into a new
  interface: a new part of the contract's `Implementation`, or an
  optional interface that the caller finds with a type assertion. A call
  into a part that an implementation leaves nil returns
  `ErrNotConfigured`.
- Every request and every result starts with the field `_ incomparable`.
  Each package declares `type incomparable [0]func()` once, as `net/http`
  does. The compiler then refuses `==` on the type, so a later slice field
  does not break a comparison. As the first field it does not change the
  size of the type. As the last field it adds padding.
- A caller writes keyed literals, such as `&ledger.HeadRequest{Stream: s}`.
  Vet's `composites` analyzer reports an unkeyed literal of another
  package's struct, which a new field breaks.

## Allocations

- A pointer that a call passes to an interface method escapes to the
  heap. A request in a local variable of the caller then costs one
  allocation per call.
- A caller passes an interface method only a request from memory that it
  reuses: a pooled call state, a pooled request such as `Batch`, or a
  long-lived object.
- A concrete method that a host calls copies the host's request into a
  pooled call state, and passes that state to the interface. The host's
  request then remains on the host's stack.
- A hand-written allocation spec of each call measures the call with the
  request in a local variable, as a host writes it. The budget of the call
  does not count that escape.

## Costs

One call through an interface with a 96-byte request, measured with
go1.27.2 on an AMD Ryzen 9 9950X3D at 4 cores, median of 6 runs:

| Form | ns per call | Allocations per call |
|---|---|---|
| positional parameters | 1.87 | 0 |
| a request that the caller reuses, by pointer | 1.39 | 0 |
| a request by value | 6.70 | 0 |
| a request in the caller's local variable, by pointer | 18.7 | 1 |
| a request copied into a pooled call state | 11.3 | 0 |

A result of 88 bytes returned by value takes 7.1 ns more than an
`(int, error)` result and does not allocate. At the throughput target of
10^6 entries per second per core, an Append of 100 entries has 100 µs.
A copy of 10 ns is then 0.01% of the Append.
