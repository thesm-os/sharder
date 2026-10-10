---
adr: 0004
title: Proposals and approvals form package proposal
status: Superseded
date: 2026-10-10
supersedes: none
superseded-by: ADR-0008
rfc: RFC-0001, RFC-0002, RFC-0007, RFC-0008
---

<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# ADR-0004: Proposals and approvals form package proposal

## Status

Superseded by [ADR-0008](0008-declarations-of-the-support-matrix.md)

## Context

In the design, package `config` contains two groups of declarations:

- The spec, the registry, the support matrix, `Resolve` and `Change` configure a namespace and check its configuration.
- `OperationKind`, `Proposal`, `Signed`, `Approvals`, `ApprovalPolicy`, `Party`, `PublicKey` and `Trust` describe the changes that an operator proposes and the approvers that a change needs.

Package `protocol` imports `config` for one type: `Submit` takes a `config.Signed`. Every transport adapter imports `protocol`, so it also depends on the registry and on `Resolve`. A transport does not call either of them. The approvals also make `config` import core's `crypto` and `crypto/sign`.

## Decision

We will move `OperationKind`, `Proposal`, `Signed`, `Approvals`, `ApprovalPolicy`, `Party`, `PublicKey` and `Trust` from `config` into a new package `proposal`, because `protocol` and its transport adapters then depend on the proposals alone.

- `proposal` imports `shard`, and `config` imports `proposal`.
- `config.Spec.Approvals` has the type `proposal.Approvals`.
- `protocol` imports `proposal` in place of `config`.
- `protocol.ControllerServer.Submit`, `protocol.ControllerClient.Submit`, `controller.Controller.Submit` and the `Submit` of the in-process transport take a `proposal.Signed`.
- `controller.WithTrust` takes a `proposal.Trust`, and `replica.Config.Trust` has the type `*proposal.Trust`.
- kanon generates the codecs of `Proposal` and `Signed` in package `proposal`.

## Alternatives Considered

### Proposals and approvals in config

Package `config` declares the proposals next to the specs that they change, as in the design. The configuration of a namespace and the way an operator changes it are then in one package.

Rejected because `protocol` and every transport adapter would depend on the registry and on `Resolve` to send one message type.

## Consequences

**Positive:**

- `protocol` and the transport adapters do not import `config`.
- `config` imports neither `crypto` nor `crypto/sign`.

**Negative:**

- `config.Spec` takes the type of its approvals section from another package.
- The root module has one more package.

**Neutral:**

- `config` imports `proposal`, because a spec contains its approvals section. `proposal` does not import `config`, because a proposal contains a spec only as JSON.
