---
"go.thesmos.sh/sharder": patch
---

Fix the controller to defer the moves of a shard for a backoff after a move of the shard aborts without a fault.
