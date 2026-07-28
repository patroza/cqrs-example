/**
 * Demo: commands, queries, sagas, event handlers, and catch-up.
 *
 *   pnpm demo
 */

import { NodeRuntime } from "@effect/platform-node"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"

import { AppLayer } from "./appLayer.ts"
import { subscribeWithCatchUp } from "./client/catchUp.ts"
import { AuditLog } from "./cqrs/eventHandlers.ts"
import { QueryBus } from "./cqrs/QueryBus.ts"
import {
  AddTodoCommand,
  CompleteTodoCommand,
  CreateListCommand,
} from "./domain/commands.ts"
import { ListId, TodoId } from "./domain/ids.ts"
import { findList } from "./domain/readModel.ts"
import { Engine } from "./engine/Engine.ts"
import { waitUntil } from "./effect/waitUntil.ts"

const program = Effect.gen(function* () {
  const engine = yield* Engine
  const queryBus = yield* QueryBus
  const auditLog = yield* AuditLog

  const listId = ListId.make("list_home")
  const milk = TodoId.make("todo_milk")
  const eggs = TodoId.make("todo_eggs")

  const createList = CreateListCommand.make({ listId, title: "Groceries" })
  const commands = [
    createList,
    AddTodoCommand.make({ listId, todoId: milk, text: "Milk" }),
    AddTodoCommand.make({ listId, todoId: eggs, text: "Eggs" }),
    CompleteTodoCommand.make({ listId, todoId: milk }),
  ]

  yield* Console.log("=== CommandBus.dispatch (Schema .make) ===")
  for (const command of commands) {
    const result = yield* engine.dispatch(command)
    yield* Console.log(
      `  ${command.type} id=${command.commandId.slice(0, 12)}… → sequence ${result.sequence}`,
    )
  }

  const again = yield* engine.dispatch(createList)
  yield* Console.log(`  re-dispatch list.create → sequence ${again.sequence} (idempotent)`)

  const rejected = yield* Effect.result(
    engine.dispatch(CompleteTodoCommand.make({ listId, todoId: milk })),
  )
  yield* Console.log(
    `  complete already-done milk → ${
      Result.isFailure(rejected)
        ? `${rejected.failure._tag}: ${"detail" in rejected.failure ? rejected.failure.detail : ""}`
        : "unexpected success"
    }`,
  )

  yield* Console.log("\n=== QueryBus.execute ===")
  const listQuery = yield* queryBus.execute({ type: "list.get", listId })
  yield* Console.log(`  list.get → ${JSON.stringify(listQuery.value)}`)

  yield* Console.log("\n=== Saga (todo.completed → list.archive) ===")
  yield* engine.dispatch(CompleteTodoCommand.make({ listId, todoId: eggs }))
  yield* waitUntil(
    Effect.gen(function* () {
      const model = yield* engine.getReadModel()
      return Option.exists(findList(model, listId), (list) => list.archived)
    }),
    { detail: `list ${listId} was not archived in time` },
  )
  const afterSaga = yield* queryBus.execute({ type: "list.get", listId })
  yield* Console.log(`  after saga → ${JSON.stringify(afterSaga.value)}`)

  const snapshot = yield* queryBus.execute({ type: "snapshot.get" })
  yield* Console.log("\n=== Snapshot query ===")
  yield* Console.log(JSON.stringify(snapshot.value, null, 2))

  const audit = yield* auditLog.entries()
  yield* Console.log("\n=== Event handler (AuditLog) — not the projector ===")
  for (const entry of audit) {
    yield* Console.log(`  #${entry.sequence} ${entry.eventType}`)
  }

  const allEvents = yield* engine.replayFrom(0)
  yield* Console.log("\n=== Event log (source of truth) ===")
  for (const event of allEvents) {
    yield* Console.log(`  #${event.sequence} ${event.type}`)
  }

  yield* Console.log("\n=== Client catch-up afterSequence=2 ===")
  const client = yield* subscribeWithCatchUp({ engine, afterSequence: 2 })
  const first = yield* Queue.take(client.updates)
  yield* Console.log(
    `  mode=${JSON.stringify(first.lastMode)} snapshotSequence=${first.model.snapshotSequence}`,
  )
  yield* Console.log(`  lists in HashMap: ${HashMap.size(first.model.lists)}`)

  yield* Console.log("\nDone.")
}).pipe(Effect.scoped, Effect.provide(AppLayer))

NodeRuntime.runMain(program)
