flux

append events. fold state. subscribe from a position. snapshot when the fold gets long. I keep writing that sentence because that is the whole library.

The store is in memory and it checks expected version, so two writers cannot silently overwrite each other. you can fold a stream, or rebuild from a snapshot (prefix skipped, tail folded, same final state). catch-up subscription starts from a global position. `flux demo` snapshots mid stream and proves the skip.

workspace crates: flux-core, flux-store, flux-project, flux-subscribe, flux-snapshot, flux-cli

still missing a disk or sqlite backend, multi process consumer groups, and retention policies. I am not calling this CQRS.

cargo test --workspace
cargo build -p flux-cli
./target/debug/flux demo

mit. expected versions exist for a reason.
