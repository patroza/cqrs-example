# CQRS / Event Sourcing sample (Effect)

A minimal **Effect** todo app that combines:

1. **T3-style event sourcing** — durable event log, pure decider/projector, snapshots + sequence catch-up  
2. **Nest-style CQRS surfaces** — QueryBus, side-effect event handlers, sagas (event → command)

Inspired by [T3 Code orchestration](https://github.com/pingdotgg/t3code) and the
[NestJS CQRS recipe](https://docs.nestjs.com/recipes/cqrs).

## Two layers of the pattern

| Concern | This sample | NestJS CQRS | T3 Code |
|---|---|---|---|
| Command | `Engine.dispatch` | `CommandBus.execute` | `orchestration.dispatchCommand` |
| Decide / handle write | pure `decider.ts` | `@CommandHandler` | `decider.ts` |
| Domain events | event store + `sequence` | `EventBus` / aggregate `apply` | `orchestration_events` |
| **Projector** (state fold) | pure `projector.ts` | often ad hoc / repo write | `projector.ts` + SQL projections |
| **Event handler** (side effects) | `AuditLog` handler | `@EventsHandler` | reactors (partial overlap) |
| **Saga** | `ArchiveWhenAllCompleteSaga` | `@Saga()` → new commands | reactors / process managers |
| **Query** | `QueryBus.execute` | `QueryBus` + `@QueryHandler` | snapshot/query services |
| Snapshot + catch-up | `subscribeWithCatchUp` | not in Nest recipe | shell/thread subscribe |
| Idempotency | `CommandReceipts` | not in Nest recipe | command receipts |

Nest’s recipe is primarily **message buses** (command / query / event + sagas).  
This sample keeps that teaching surface, but the **source of truth is still the event log**.

### Flow

```text
Client
  │  CommandBus: dispatch(command)
  │  QueryBus:   execute(query)  ──────────────────┐
  ▼                                                │
Engine (serialized command worker)                 │
  │  1. receipt lookup (idempotent)                │
  │  2. decide(command, readModel) → events        │
  │  3. eventStore.append → sequence               │
  │  4. projectEvent → read model  ◄── pure fold   │
  │  5. publish EventBus stream                    │
  │  6. event handlers (AuditLog, …)  ◄── side FX  │
  │  7. sagas → enqueue follow-up commands         │
  ▼                                                │
Read model ────────────────────────────────────────┘
  snapshot @ N → replay (N+1…M) → live
```

## Quick start

```bash
cd ~/pj/cqrs-example
pnpm install
pnpm demo
pnpm test
pnpm typecheck
```

## Nest-style pieces added

### QueryBus

```ts
const result = yield* queryBus.execute({ type: "list.get", listId })
// → pure handleQuery over the projected read model
```

Queries never go through the command worker. See `src/cqrs/queries.ts` and
`src/cqrs/QueryBus.ts`.

### Event handlers (≠ projector)

```ts
// After each committed event:
AuditLogHandler records { sequence, eventType, … }
// Failures → UnhandledExceptionBus (Nest-like)
```

The **projector** rebuilds lists/todos. The **audit handler** only observes.
See `src/cqrs/eventHandlers.ts`.

### Sagas

When every todo on a list is completed, `ArchiveWhenAllCompleteSaga` enqueues:

```ts
{ type: "list.archive", commandId: "saga:archive:<listId>:@<seq>", listId }
```

That command is processed asynchronously on the same command queue (no
self-deadlock). See `src/cqrs/sagas.ts`.

## Layout

```text
src/
  domain/
    types.ts         # Command, DomainEvent, ReadModel, Snapshot
    errors.ts
    decider.ts       # pure command + state → events
    projector.ts     # pure event → read model
  cqrs/
    queries.ts       # Query types + pure handlers
    QueryBus.ts      # Nest QueryBus analogue
    eventHandlers.ts # AuditLog + UnhandledExceptionBus
    sagas.ts         # event → follow-up commands
  persistence/
    EventStore.ts
    CommandReceipts.ts
  engine/
    Engine.ts        # CommandBus + EventBus core
  client/
    catchUp.ts       # snapshot / replay / live
  main.ts
  appLayer.ts
test/
  engine.test.ts
```

## What is still simplified

| Full system | This sample |
|---|---|
| Nest `AggregateRoot.apply/commit` | Functional decider (T3-style) |
| Nest request-scoped handlers | Single process, no HTTP scope |
| SQLite / EventStoreDB | In-memory event store |
| Multiple SQL read models | One in-memory `ReadModel` |
| RxJS saga streams | Sync `react(event, model) → commands[]` |

## License

MIT — free to copy into other teaching repos.
