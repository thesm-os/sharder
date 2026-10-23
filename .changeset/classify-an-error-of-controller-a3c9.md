---
"go.thesmos.sh/sharder": patch
---

Classify an error of `controller.Controller.Do` that wraps `context.DeadlineExceeded` as `errs.Transient`.
