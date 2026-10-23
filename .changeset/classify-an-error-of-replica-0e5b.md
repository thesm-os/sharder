---
"go.thesmos.sh/sharder": patch
---

Classify an error of `replica.Agent.Do` that wraps `context.DeadlineExceeded` as `errs.Transient`.
