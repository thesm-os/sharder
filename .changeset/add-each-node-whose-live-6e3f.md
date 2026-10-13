---
"go.thesmos.sh/sharder": patch
---

Add each node whose live incarnation failed an add of a shard definitively to the `placement.Input.Faults` of the shard.
