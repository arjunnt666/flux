# Status

## works today
- in-memory event store append/read with optimistic concurrency
- fold / rebuild aggregate state
- catch-up subscriptions from global position
- snapshots roundtrip
- `flux demo` end to end
- unit tests in store, project, subscribe, snapshot

## does not work yet
- durable disk/sqlite backend
- multi-process subscription groups
- production hardening

CI now fails if the workspace does not compile or tests fail.
