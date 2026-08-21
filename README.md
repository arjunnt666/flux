# flux

event sourcing core without the full CQRS religion.

append events. fold state. subscribe from a position. snapshot when the fold gets long. that is the whole pitch.

not a platform. not outbox + sagas + projections-as-a-service. a small embeddable log with optimistic concurrency so two writers cannot silently overwrite each other.

## works today

- in-memory event store with expected version checks
- fold / rebuild for a stream
- catch-up subscription from a global position
- snapshots
- `flux demo` appends, folds, and reads the tail

## does not work yet

- durable disk or sqlite backend
- multi-process consumer groups
- production retention policies

## try it

```bash
cargo test --workspace
cargo build -p flux-cli
./target/debug/flux demo
```

## crates

flux-core, flux-store, flux-project, flux-subscribe, flux-snapshot, flux-cli

## license

mit. append carefully. expected versions exist for a reason.
