import assert from "node:assert/strict"
import { describe, it } from "node:test"

import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import type * as Scope from "effect/Scope"

import { AppLayer } from "../src/appLayer.ts"
import { subscribeWithCatchUp } from "../src/client/catchUp.ts"
import { AuditLog } from "../src/cqrs/eventHandlers.ts"
import { QueryBus } from "../src/cqrs/QueryBus.ts"
import {
  AddTodoCommand,
  CompleteTodoCommand,
  CreateListCommand,
} from "../src/domain/commands.ts"
import { CommandId, ListId, TodoId } from "../src/domain/ids.ts"
import { rebuildFromEvents } from "../src/domain/projector.ts"
import { findList } from "../src/domain/readModel.ts"
import { waitUntil } from "../src/effect/waitUntil.ts"
import { Engine } from "../src/engine/Engine.ts"

const run = <A, E>(
  effect: Effect.Effect<A, E, Engine | QueryBus | AuditLog | Scope.Scope>,
) => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(AppLayer)))

const listId = ListId.make("list_1")
const todoId = TodoId.make("todo_1")

describe("CQRS/ES engine", () => {
  it("Schema command .make auto-fills type and commandId", () => {
    const a = CreateListCommand.make({ listId, title: "Work" })
    const b = CreateListCommand.make({ listId, title: "Work" })

    assert.equal(a.type, "list.create")
    assert.equal(a.title, "Work")
    assert.ok(a.commandId.startsWith("cmd_"))
    assert.notEqual(a.commandId, b.commandId)
    assert.ok(a instanceof CreateListCommand)

    const fixed = CreateListCommand.make({
      listId,
      title: "Fixed",
      commandId: CommandId.make("cmd_explicit"),
    })
    assert.equal(fixed.commandId, "cmd_explicit")
  })

  it("decides, appends, projects, and exposes a snapshot", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine

        yield* engine.dispatch(CreateListCommand.make({ listId, title: "Work" }))
        yield* engine.dispatch(
          AddTodoCommand.make({ listId, todoId, text: "Ship sample" }),
        )

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
        const orphan = AddTodoCommand.make({
          listId,
          todoId,
          text: "orphan",
        })

        const result = yield* Effect.result(engine.dispatch(orphan))

        assert.equal(Result.isFailure(result), true)
        if (Result.isFailure(result)) {
          assert.equal(result.failure._tag, "CommandInvariantError")
        }

        const again = yield* Effect.result(engine.dispatch(orphan))

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
        const create = CreateListCommand.make({
          listId: ListId.make("list_idem"),
          title: "Once",
        })

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
        const lid = ListId.make("list_rebuild")
        const tid = TodoId.make("todo_rebuild")

        yield* engine.dispatch(CreateListCommand.make({ listId: lid, title: "Rebuild" }))
        yield* engine.dispatch(AddTodoCommand.make({ listId: lid, todoId: tid, text: "A" }))
        yield* engine.dispatch(CompleteTodoCommand.make({ listId: lid, todoId: tid }))

        yield* waitUntil(
          Effect.gen(function* () {
            const model = yield* engine.getReadModel()
            return Option.exists(findList(model, lid), (list) => list.archived)
          }),
        )

        const live = yield* engine.getReadModel()
        const events = yield* engine.replayFrom(0)
        const rebuilt = rebuildFromEvents(events)

        assert.equal(rebuilt.snapshotSequence, live.snapshotSequence)
        assert.deepEqual(
          Option.getOrUndefined(findList(rebuilt, lid))?.todos,
          Option.getOrUndefined(findList(live, lid))?.todos,
        )
        assert.equal(Option.getOrUndefined(findList(rebuilt, lid))?.archived, true)
      }),
    )
  })

  it("catch-up replays from afterSequence then applies live", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const lid = ListId.make("list_catchup")
        const t1 = TodoId.make("t1")
        const t2 = TodoId.make("t2")

        yield* engine.dispatch(CreateListCommand.make({ listId: lid, title: "Catchup" }))
        yield* engine.dispatch(AddTodoCommand.make({ listId: lid, todoId: t1, text: "one" }))

        const client = yield* subscribeWithCatchUp({ engine, afterSequence: 1 })
        const hydrated = yield* Queue.take(client.updates)
        assert.equal(hydrated.lastMode.kind, "replay")
        assert.equal(hydrated.model.snapshotSequence, 2)

        yield* engine.dispatch(AddTodoCommand.make({ listId: lid, todoId: t2, text: "two" }))

        const live = yield* Queue.take(client.updates)
        assert.equal(live.lastMode.kind, "live")
        const list = findList(live.model, lid)
        assert.equal(Option.isSome(list) && list.value.todos.length, 2)
      }),
    )
  })

  it("QueryBus reads via query handlers (not commands)", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const queryBus = yield* QueryBus
        const lid = ListId.make("list_query")

        yield* engine.dispatch(CreateListCommand.make({ listId: lid, title: "Queryable" }))

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
        const lid = ListId.make("list_audit")

        yield* audit.clear()
        yield* engine.dispatch(CreateListCommand.make({ listId: lid, title: "Audited" }))

        const entries = yield* audit.entries()
        assert.ok(entries.some((e) => e.eventType === "list.created" && e.aggregateId === lid))

        const list = findList(yield* engine.getReadModel(), lid)
        assert.equal(Option.getOrUndefined(list)?.title, "Audited")
        assert.equal(HashMap.size((yield* engine.getReadModel()).lists) >= 1, true)
      }),
    )
  })

  it("saga archives list when all todos complete", async () => {
    await run(
      Effect.gen(function* () {
        const engine = yield* Engine
        const queryBus = yield* QueryBus
        const lid = ListId.make("list_saga")
        const t1 = TodoId.make("s1")
        const t2 = TodoId.make("s2")

        yield* engine.dispatch(CreateListCommand.make({ listId: lid, title: "Saga list" }))
        yield* engine.dispatch(AddTodoCommand.make({ listId: lid, todoId: t1, text: "one" }))
        yield* engine.dispatch(AddTodoCommand.make({ listId: lid, todoId: t2, text: "two" }))
        yield* engine.dispatch(CompleteTodoCommand.make({ listId: lid, todoId: t1 }))

        assert.equal(
          Option.getOrUndefined(findList(yield* engine.getReadModel(), lid))?.archived,
          false,
        )

        yield* engine.dispatch(CompleteTodoCommand.make({ listId: lid, todoId: t2 }))

        yield* waitUntil(
          Effect.gen(function* () {
            const model = yield* engine.getReadModel()
            return Option.exists(findList(model, lid), (list) => list.archived)
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
