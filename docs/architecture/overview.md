# Architecture

flux splits event sourcing plumbing into small crates:

1. core event and stream types
2. store append/read with optimistic concurrency
3. project fold and catch-up helpers
4. subscribe from global position
5. snapshot optional skip-replay state
6. cli demos

streams are ordered per aggregate id.
global position is a monotonic counter across streams for projections.
