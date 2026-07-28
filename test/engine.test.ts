import assert from "node:assert/strict"
import { describe, it } from "node:test"

import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import type * as Scope from "effect/Scope"

import { AppLayer } from "../src/appLayer.ts"
import { subscribeWithCatchUp } from "../src/client/catchUp.ts"
import { Engine } from "../src/engine/Engine.ts"
import { rebuildFromEvents } from "../src/domain/projector.ts"
import type { Command, CommandId, ListId, TodoId } from "../src/domain/types.ts"

const run = <A, E>(effect: Effect.Effect<A, E, Engine | Scope.Scope>) =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(AppLayer)))

let n = 0
const commandId = (): CommandId => `cmd_${++n}` as CommandId
const listId = "list_1" as ListId
const todoId = "todo_1" as TodoId

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

        const live = yield* engine.getReadModel()
        const events = yield* engine.replayFrom(0)
        const rebuilt = rebuildFromEvents(events)

        assert.equal(rebuilt.snapshotSequence, live.snapshotSequence)
        assert.deepEqual(rebuilt.lists.get(lid)?.todos, live.lists.get(lid)?.todos)
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
})
