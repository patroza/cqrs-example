/**
 * Demo: run a few todo commands and show snapshot / replay / live catch-up.
 *
 *   pnpm demo
 */

import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"

import { AppLayer } from "./appLayer.ts"
import { subscribeWithCatchUp } from "./client/catchUp.ts"
import { Engine } from "./engine/Engine.ts"
import type { Command, CommandId, ListId, TodoId } from "./domain/types.ts"

let commandCounter = 0
const cmdId = (): CommandId => `cmd_${++commandCounter}` as CommandId

const program = Effect.gen(function* () {
  const engine = yield* Engine

  const listId = "list_home" as ListId
  const milk = "todo_milk" as TodoId
  const eggs = "todo_eggs" as TodoId

  const commands: Command[] = [
    {
      type: "list.create",
      commandId: cmdId(),
      listId,
      title: "Groceries",
    },
    {
      type: "todo.add",
      commandId: cmdId(),
      listId,
      todoId: milk,
      text: "Milk",
    },
    {
      type: "todo.add",
      commandId: cmdId(),
      listId,
      todoId: eggs,
      text: "Eggs",
    },
    {
      type: "todo.complete",
      commandId: cmdId(),
      listId,
      todoId: milk,
    },
  ]

  yield* Console.log("=== Dispatching commands ===")
  for (const command of commands) {
    const result = yield* engine.dispatch(command)
    yield* Console.log(`  ${command.type} → sequence ${result.sequence}`)
  }

  // Idempotency: same commandId is a no-op accept.
  const again = yield* engine.dispatch(commands[0]!)
  yield* Console.log(`  re-dispatch list.create → sequence ${again.sequence} (idempotent)`)

  // Invariant failure.
  const rejected = yield* Effect.result(
    engine.dispatch({
      type: "todo.complete",
      commandId: cmdId(),
      listId,
      todoId: milk,
    }),
  )
  yield* Console.log(
    `  complete already-done milk → ${
      Result.isFailure(rejected)
        ? `${rejected.failure._tag}: ${"detail" in rejected.failure ? rejected.failure.detail : ""}`
        : "unexpected success"
    }`,
  )

  const snapshot = yield* engine.getSnapshot()
  yield* Console.log("\n=== Snapshot (query side) ===")
  yield* Console.log(JSON.stringify(snapshot, null, 2))

  const allEvents = yield* engine.replayFrom(0)
  yield* Console.log("\n=== Event log (source of truth) ===")
  for (const event of allEvents) {
    yield* Console.log(`  #${event.sequence} ${event.type}`)
  }

  // Catch-up from sequence 2: client has events 1–2 cached, replays 3+.
  yield* Console.log("\n=== Client catch-up afterSequence=2 ===")
  const client = yield* subscribeWithCatchUp({ engine, afterSequence: 2 })
  const first = yield* Queue.take(client.updates)
  yield* Console.log(
    `  mode=${JSON.stringify(first.lastMode)} snapshotSequence=${first.model.snapshotSequence}`,
  )
  yield* Console.log(
    `  todos: ${JSON.stringify(
      first.model.lists.get(listId)?.todos.map((t) => ({
        text: t.text,
        completed: t.completed,
      })),
    )}`,
  )

  // Live event after subscribe.
  yield* engine.dispatch({
    type: "todo.rename",
    commandId: cmdId(),
    listId,
    todoId: eggs,
    text: "Free-range eggs",
  })
  const live = yield* Queue.take(client.updates)
  yield* Console.log(
    `  live update mode=${JSON.stringify(live.lastMode)} sequence=${live.model.snapshotSequence}`,
  )
  yield* Console.log(
    `  todos: ${JSON.stringify(
      live.model.lists.get(listId)?.todos.map((t) => ({
        text: t.text,
        completed: t.completed,
      })),
    )}`,
  )

  yield* Console.log("\nDone.")
})

Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(AppLayer))).then(
  () => undefined,
  (error: unknown) => {
    console.error(error)
    process.exit(1)
  },
)
