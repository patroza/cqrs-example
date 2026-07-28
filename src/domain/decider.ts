/**
 * Decider — pure command → event(s) given current read model.
 *
 * Same role as T3's `apps/server/src/orchestration/decider.ts`:
 * no I/O beyond id/time, only invariants + event construction.
 */

import * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"

import { CommandInvariantError } from "./errors.ts"
import type {
  Command,
  CommandId,
  EventId,
  ListId,
  ReadModel,
  UnsequencedEvent,
} from "./types.ts"

export type DecideInput = {
  readonly command: Command
  readonly readModel: ReadModel
}

const newEventId = Effect.fn("decider.newEventId")(function* () {
  const crypto = yield* Crypto.Crypto
  // Platform crypto failures are defects for this sample.
  const id = yield* crypto.randomUUIDv4.pipe(Effect.orDie)
  return `evt_${id}` as EventId
})

const nowIso = Effect.map(DateTime.now, DateTime.formatIso)

const requireList = (readModel: ReadModel, listId: ListId, commandType: string) => {
  const list = readModel.lists.get(listId)
  if (!list) {
    return Effect.fail(
      new CommandInvariantError({
        commandType,
        detail: `List ${listId} does not exist.`,
      }),
    )
  }
  return Effect.succeed(list)
}

/**
 * Turn a command + current state into one or more unsequenced domain events.
 */
export const decide = Effect.fn("decider.decide")(function* (input: DecideInput) {
  const { command, readModel } = input
  const eventId = yield* newEventId()
  const occurredAt = yield* nowIso
  const commandId = command.commandId as CommandId

  switch (command.type) {
    case "list.create": {
      if (readModel.lists.has(command.listId)) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} already exists.`,
        })
      }
      if (command.title.trim().length === 0) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: "List title must not be empty.",
        })
      }
      return [
        {
          eventId,
          type: "list.created" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: { listId: command.listId, title: command.title.trim() },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }

    case "todo.add": {
      const list = yield* requireList(readModel, command.listId, command.type)
      if (list.archived) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} is archived.`,
        })
      }
      if (list.todos.some((t) => t.id === command.todoId)) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `Todo ${command.todoId} already exists on list ${command.listId}.`,
        })
      }
      if (command.text.trim().length === 0) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: "Todo text must not be empty.",
        })
      }
      return [
        {
          eventId,
          type: "todo.added" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: {
            listId: command.listId,
            todoId: command.todoId,
            text: command.text.trim(),
          },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }

    case "todo.complete": {
      const list = yield* requireList(readModel, command.listId, command.type)
      if (list.archived) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} is archived.`,
        })
      }
      const todo = list.todos.find((t) => t.id === command.todoId)
      if (!todo) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `Todo ${command.todoId} not found on list ${command.listId}.`,
        })
      }
      if (todo.completed) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `Todo ${command.todoId} is already completed.`,
        })
      }
      return [
        {
          eventId,
          type: "todo.completed" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: { listId: command.listId, todoId: command.todoId },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }

    case "todo.rename": {
      const list = yield* requireList(readModel, command.listId, command.type)
      if (list.archived) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} is archived.`,
        })
      }
      const todo = list.todos.find((t) => t.id === command.todoId)
      if (!todo) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `Todo ${command.todoId} not found on list ${command.listId}.`,
        })
      }
      if (command.text.trim().length === 0) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: "Todo text must not be empty.",
        })
      }
      if (todo.text === command.text.trim()) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: "Todo text is unchanged.",
        })
      }
      return [
        {
          eventId,
          type: "todo.renamed" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: {
            listId: command.listId,
            todoId: command.todoId,
            text: command.text.trim(),
          },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }

    case "todo.remove": {
      const list = yield* requireList(readModel, command.listId, command.type)
      if (list.archived) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} is archived.`,
        })
      }
      if (!list.todos.some((t) => t.id === command.todoId)) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `Todo ${command.todoId} not found on list ${command.listId}.`,
        })
      }
      return [
        {
          eventId,
          type: "todo.removed" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: { listId: command.listId, todoId: command.todoId },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }

    case "list.archive": {
      const list = yield* requireList(readModel, command.listId, command.type)
      if (list.archived) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} is already archived.`,
        })
      }
      if (list.todos.length === 0) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} has no todos to archive.`,
        })
      }
      if (!list.todos.every((t) => t.completed)) {
        return yield* new CommandInvariantError({
          commandType: command.type,
          detail: `List ${command.listId} still has open todos.`,
        })
      }
      return [
        {
          eventId,
          type: "list.archived" as const,
          aggregateId: command.listId,
          commandId,
          occurredAt,
          payload: { listId: command.listId },
        },
      ] satisfies ReadonlyArray<UnsequencedEvent>
    }
  }
})
