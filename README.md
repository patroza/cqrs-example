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

## Effect idioms (v4)

This sample follows guidance from Effect’s `LLMS.md` and common app patterns
(e.g. macs scanner):

| Idiom | Where |
|---|---|
| `Context.Service` + **`static layer`** | `EventStore`, `Engine`, `QueryBus`, `DomainEvents`, … |
| `Effect.fn("Name.method")` | Service methods and helpers |
| `Schema.Class` / `Schema.tag` / `.make` | Commands |
| Branded ids via `ListId.make(...)` | `src/domain/ids.ts` |
| `HashMap` for read-model collections | `ReadModel.lists` |
| PubSub as a service (`DomainEvents`) | Like Effect’s PubSub cookbook |
| `NodeCrypto.layer` | `@effect/platform-node` (not hand-rolled) |
| `NodeRuntime.runMain` | `src/main.ts` entrypoint |
| `Schedule` polling | `waitUntil` for saga tests/demo |

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

## Commands as Schema classes

Commands are Effect `Schema.Class` values. Prefer **`.make`** so `type` and
`commandId` fill in automatically:

```ts
import { CreateListCommand, AddTodoCommand, CompleteTodoCommand } from "./src/domain/commands.ts"

// auto type + commandId
const create = CreateListCommand.make({ listId: "list_1", title: "Groceries" })

// override commandId when you need idempotency or saga correlation
const again = CreateListCommand.make({
  listId: "list_1",
  title: "Groceries",
  commandId: create.commandId,
})

await engine.dispatch(AddTodoCommand.make({
  listId: "list_1",
  todoId: "todo_1",
  text: "Milk",
}))
```

See `src/domain/commands.ts`. `Command` is the TypeScript union of command
instances; `CommandSchema` is the Effect `Schema.Union` for decoding.

## Layout

```text
src/
  domain/
    ids.ts           # branded ids (ListId.make / TodoId.make)
    commands.ts      # Schema.Class + Schema.tag + auto commandId
    types.ts         # DomainEvent, ReadModel (HashMap), Snapshot
    readModel.ts     # HashMap helpers (findList / updateList)
    errors.ts
    decider.ts       # pure command + state → events
    projector.ts     # pure event → read model
  cqrs/
    DomainEvents.ts  # PubSub service (EventBus)
    queries.ts
    QueryBus.ts      # static layer
    eventHandlers.ts # AuditLog + UnhandledExceptionBus
    sagas.ts
  persistence/
    EventStore.ts    # static layer
    CommandReceipts.ts
  engine/
    Engine.ts        # static layer; uses DomainEvents
  client/
    catchUp.ts
  effect/
    waitUntil.ts     # Schedule-based poll
  main.ts            # NodeRuntime.runMain
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
