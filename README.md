<!--
  ~ Copyright ThesmOS B.V. 2026
  ~ SPDX-License-Identifier: BUSL-1.1
-->

# sharder

A Go library that a sharded application embeds to map keys to shards, place shards on nodes and
move them safely. The RFCs in [docs/rfc](docs/rfc/README.md) propose the design, and
[docs/roadmap](docs/roadmap/README.md) orders the work.

## Support matrix

A spec selects the strategies and the presets in this table, and no others. `builtin.Support`
returns the same matrix, and a test of package `builtin` fails when the two differ.

| Axis | Supported |
|---|---|
| `partitioning` | `fixed-slots`, `singleton` |
| `placement` | `planner` |
| `replication` | `primary-only` |
| `ownership` | `exclusive` |
| `handoff` | `break-before-make` |
| `topology` | `embedded` |
| `routing` | `watch` |
| `constraint` | Every registered name |
| `goal` | Every registered name |
| `hash` | Every registered name |
| Presets | `exclusive-worker`, `consumer-group`, `actor-entities`, `leader-election` |
| Required capabilities | `fenced` |

The ownership configuration of a namespace sets `"fenced": true`, because only a fenced
`exclusive` provides the capability `fenced`.

## Development

The Makefile runs the checks through [ergon](https://github.com/dokimasia/ergon):

```sh
make check   # the gate: lint, test, race and audit in every module
make fmt     # format the sources with the formatters of .golangci.yml
make help    # list the targets
```

## Contributing

[CONTRIBUTING](CONTRIBUTING.md) describes the gate, the changesets and the commit messages.
Report a vulnerability to security@thesmos.sh, as [SECURITY](SECURITY.md) describes.

## License

The sharder is under BUSL-1.1. See [LICENSE](LICENSE).
