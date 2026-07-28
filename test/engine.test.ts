import assert from "node:assert/strict"
import { describe, it } from "node:test"

import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import type * as Scope from "effect/Scope"

import { AppLayer } from "../src/appLayer.ts"
import { subscribeWithCatchUp } from "../src/client/catchUp.ts"
import { AuditLog } from "../src/cqrs/eventHandlers.ts"
import { QueryBus } from "../src/cqrs/QueryBus.ts"
import { rebuildFromEvents } from "../src/domain/projector.ts"
import type { Command, CommandId, ListId, TodoId } from "../src/domain/types.ts"
import { Engine } from "../src/engine/Engine.ts"

const run = <A, E>(
  effect: Effect.Effect<A, E, Engine | QueryBus | AuditLog | Scope.Scope>,
) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(AppLayer)))

let n = 0
const commandId = (): CommandId => `cmd_${++n}` as CommandId
const listId = "list_1" as ListId
const todoId = "todo_1" as TodoId

const waitUntil = <R>(
  check: Effect.Effect<boolean, never, R>,
): Effect.Effect<void, Error, R> =>
  Effect.gen(function* () {
    for (let i = 0; i < 200; i++) {
      if (yield* check) return
      yield* Effect.sleep(Duration.millis(5))
    }
    return yield* Effect.fail(new Error("condition not met in time"))
  })

describe("CQRS/ES engine", () => {
  it("decides, appends, projects, and exposes a snapshot", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine

        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId,
          title: "Work",
        })
        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId,
          todoId,
          text: "Ship sample",
        })

        const snapshot = yield* engine.getSnapshot()
        assert.equal(snapshot.snapshotSequence, 2)
        assert.equal(snapshot.lists.length, 1)
        assert.equal(snapshot.lists[0]?.title, "Work")
        assert.equal(snapshot.lists[0]?.todos[0]?.text, "Ship sample")
        assert.equal(snapshot.lists[0]?.todos[0]?.completed, false)
        assert.equal(snapshot.lists[0]?.archived, false)
      }),
    )
  })

  it("rejects invalid commands and records receipt", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const id = commandId()

        const result = yield* Effect.result(
          engine.dispatch({
            type: "todo.add",
            commandId: id,
            listId,
            todoId,
            text: "orphan",
          }),
        )

        assert.equal(Result.isFailure(result), true)
        if (Result.isFailure(result)) {
          assert.equal(result.failure._tag, "CommandInvariantError")
        }

        const again = yield* Effect.result(
          engine.dispatch({
            type: "todo.add",
            commandId: id,
            listId,
            todoId,
            text: "orphan",
          }),
        )

        assert.equal(Result.isFailure(again), true)
        if (Result.isFailure(again)) {
          assert.equal(again.failure._tag, "CommandPreviouslyRejectedError")
        }
      }),
    )
  })

  it("is idempotent on accepted commandId", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const create: Command = {
          type: "list.create",
          commandId: commandId(),
          listId: "list_idem" as ListId,
          title: "Once",
        }

        const first = yield* engine.dispatch(create)
        const second = yield* engine.dispatch(create)
        assert.equal(first.sequence, second.sequence)

        const snapshot = yield* engine.getSnapshot()
        assert.equal(snapshot.lists.filter((l) => l.id === create.listId).length, 1)
      }),
    )
  })

  it("rebuilds identical state from the event log", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const lid = "list_rebuild" as ListId
        const tid = "todo_rebuild" as TodoId

        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId: lid,
          title: "Rebuild",
        })
        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId: lid,
          todoId: tid,
          text: "A",
        })
        yield* engine.dispatch({
          type: "todo.complete",
          commandId: commandId(),
          listId: lid,
          todoId: tid,
        })

        // Saga may archive after complete — wait so rebuild includes archive event.
        yield* waitUntil(
          Effect.gen(function* () {
            const model = yield* engine.getReadModel()
            return model.lists.get(lid)?.archived === true
          }),
        )

        const live = yield* engine.getReadModel()
        const events = yield* engine.replayFrom(0)
        const rebuilt = rebuildFromEvents(events)

        assert.equal(rebuilt.snapshotSequence, live.snapshotSequence)
        assert.deepEqual(rebuilt.lists.get(lid)?.todos, live.lists.get(lid)?.todos)
        assert.equal(rebuilt.lists.get(lid)?.archived, true)
      }),
    )
  })

  it("catch-up replays from afterSequence then applies live", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const lid = "list_catchup" as ListId
        const t1 = "t1" as TodoId
        const t2 = "t2" as TodoId

        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId: lid,
          title: "Catchup",
        })
        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId: lid,
          todoId: t1,
          text: "one",
        })
        // sequence should be 2 here

        const client = yield* subscribeWithCatchUp({ engine, afterSequence: 1 })
        const hydrated = yield* Queue.take(client.updates)
        assert.equal(hydrated.lastMode.kind, "replay")
        assert.equal(hydrated.model.snapshotSequence, 2)

        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId: lid,
          todoId: t2,
          text: "two",
        })

        const live = yield* Queue.take(client.updates)
        assert.equal(live.lastMode.kind, "live")
        assert.equal(live.model.lists.get(lid)?.todos.length, 2)
      }),
    )
  })

  it("QueryBus reads via query handlers (not commands)", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const queryBus = yield* QueryBus
        const lid = "list_query" as ListId

        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId: lid,
          title: "Queryable",
        })

        const result = yield* queryBus.execute({ type: "list.get", listId: lid })
        assert.equal(result.type, "list.get")
        if (result.type === "list.get") {
          assert.equal(result.value.found, true)
          if (result.value.found) {
            assert.equal(result.value.list.title, "Queryable")
          }
        }

        const lists = yield* queryBus.execute({ type: "list.list", activeOnly: true })
        assert.equal(lists.type, "list.list")
        if (lists.type === "list.list") {
          assert.ok(lists.value.some((l) => l.id === lid))
        }
      }),
    )
  })

  it("event handler audit log records events without being the projector", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const audit = yield* AuditLog
        const lid = "list_audit" as ListId

        yield* audit.clear()
        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId: lid,
          title: "Audited",
        })

        const entries = yield* audit.entries()
        assert.ok(entries.some((e) => e.eventType === "list.created" && e.aggregateId === lid))

        // Projector state still correct and independent of audit sink.
        const list = (yield* engine.getReadModel()).lists.get(lid)
        assert.equal(list?.title, "Audited")
      }),
    )
  })

  it("saga archives list when all todos complete", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const queryBus = yield* QueryBus
        const lid = "list_saga" as ListId
        const t1 = "s1" as TodoId
        const t2 = "s2" as TodoId

        yield* engine.dispatch({
          type: "list.create",
          commandId: commandId(),
          listId: lid,
          title: "Saga list",
        })
        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId: lid,
          todoId: t1,
          text: "one",
        })
        yield* engine.dispatch({
          type: "todo.add",
          commandId: commandId(),
          listId: lid,
          todoId: t2,
          text: "two",
        })
        yield* engine.dispatch({
          type: "todo.complete",
          commandId: commandId(),
          listId: lid,
          todoId: t1,
        })

        // Not archived yet — one open todo remains.
        assert.equal((yield* engine.getReadModel()).lists.get(lid)?.archived, false)

        yield* engine.dispatch({
          type: "todo.complete",
          commandId: commandId(),
          listId: lid,
          todoId: t2,
        })

        yield* waitUntil(
          Effect.gen(function* () {
            const model = yield* engine.getReadModel()
            return model.lists.get(lid)?.archived === true
          }),
        )

        const q = yield* queryBus.execute({ type: "list.get", listId: lid })
        assert.equal(q.type, "list.get")
        if (q.type === "list.get" && q.value.found) {
          assert.equal(q.value.list.archived, true)
        }

        const events = yield* engine.replayFrom(0)
        assert.ok(events.some((e) => e.type === "list.archived" && e.payload.listId === lid))
      }),
    )
  })
})
