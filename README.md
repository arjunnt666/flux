# flux

event sourcing core.

append only event log. projections. subscriptions.
embeddable. not a full cqrs platform. not eventstoredb. not a message bus.

if your domain already thinks in facts that happened, flux is a small shape for that.
stream per aggregate. ordered events. rebuild read models. optional snapshots so you do not replay forever.

## why

most teams reinvent a thin event log inside the app.
then they invent projection lag, dual writes, and a temporary table that becomes permanent.

flux is the boring middle:
- append events to a stream
- load stream history
- fold into state
- project into secondary views
- subscribe from a position

you still own the domain events and the projection logic.
flux owns the plumbing shape.

## status

early skeleton with real fold and concurrency logic in the core path.
in memory store works for tests and demos.
durable backends are sketched, not production finished.

do not put your ledger of record on this yet.
do poke at how the crates are split if you care about event sourced boundaries.

## crates

- flux-core events, streams, envelopes, errors
- flux-store append and read traits + memory backend
- flux-project fold helpers and projection registry
- flux-subscribe catch up subscriptions
- flux-snapshot optional aggregate snapshots
- flux-cli local demos

js and python packages under packages for reading exported json.

## quick start

```bash
cargo build -p flux-cli
./target/debug/flux version
./target/debug/flux demo
```

## license

mit. append carefully. replay honestly.
