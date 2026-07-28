# CQRS / Event Sourcing sample (Effect)

A minimal **Effect** todo app that applies the same orchestration principles as
[T3 Code](https://github.com/pingdotgg/t3code): commands, a pure decider, a
durable event log as source of truth, a projector into a read model, snapshots
with a sequence cursor, and client catch-up (replay + live).

This is **not** a copy of T3 — it is a small teaching extract of the pattern.

## Principles (mapped to T3)

| Concept | This sample | T3 Code |
|---|---|---|
| Command | `list.create`, `todo.add`, … | `thread.create`, `thread.turn.start`, … |
| Decider | `src/domain/decider.ts` | `apps/server/src/orchestration/decider.ts` |
| Domain event | `list.created`, `todo.added`, … | `thread.created`, `thread.message-sent`, … |
| Event store | `src/persistence/EventStore.ts` (in-memory) | SQLite `orchestration_events` |
| Projector | `src/domain/projector.ts` | `apps/server/src/orchestration/projector.ts` |
| Engine | `src/engine/Engine.ts` | `OrchestrationEngine` |
| Snapshot + sequence | `getSnapshot()` → `snapshotSequence` | shell/thread projection snapshots |
| Replay / catch-up | `replayFrom` + `subscribeWithCatchUp` | `subscribeShell` / `subscribeThread` |
| Idempotency | `CommandReceipts` by `commandId` | command receipt repository |
| Live stream | PubSub of domain events | `ServerPushBus` / domain event stream |

### Flow

```text
Client
  │  dispatch(command)
  ▼
Engine (serialized worker)
  │  1. receipt lookup (idempotent)
  │  2. decide(command, readModel)  → unsequenced events
  │  3. eventStore.append           → sequence assigned
  │  4. projectEvent                → in-memory read model
  │  5. publish                     → live subscribers
  ▼
Client catch-up
  snapshot @ N  →  replay (N+1…M)  →  live (M+1…)
  (dedupe by sequence)
```

**Events are the source of truth.** The read model is always a fold of events.
Snapshots are a convenience so clients do not replay the entire log.

## Quick start

```bash
cd ~/pj/cqrs-example
pnpm install
pnpm demo
pnpm test
pnpm typecheck
```

## Layout

```text
src/
  domain/
    types.ts        # Command, DomainEvent, ReadModel, Snapshot
    errors.ts       # CommandInvariantError, …
    decider.ts      # pure command + state → events
    projector.ts    # pure event → read model
  persistence/
    EventStore.ts   # append + readFromSequence
    CommandReceipts.ts
  engine/
    Engine.ts       # dispatch pipeline + live stream
  client/
    catchUp.ts      # snapshot / replay / live subscription
  main.ts           # runnable demo
  index.ts
test/
  engine.test.ts
```

## What is intentionally simplified

| T3 | This sample |
|---|---|
| SQLite event store + multi-projector SQL tables | In-memory store + single read model |
| Separate command-side HashMap model vs query projections | One `ReadModel` used for both |
| Reactors (provider, checkpoints, worktrees) | None — pure domain only |
| WebSocket subscribe API | In-process `subscribeWithCatchUp` |
| Provider runtime → commands ingestion | Direct `dispatch` only |

The **shape of the pattern** is what matters: decide → append → project →
snapshot/replay with a monotonic `sequence`.

## Mental model

```text
┌──────────────────────────────────────┐
│  Event store (truth)                 │
│  events[sequence, type, payload…]    │
└──────────────────┬───────────────────┘
                   │ project
                   ▼
            Read model / Snapshot
            (lists, todos, snapshotSequence)
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
   Command side           Client catch-up
   (decide invariants)    snapshot + replay + live
```

## License

MIT — free to copy into other teaching repos.
