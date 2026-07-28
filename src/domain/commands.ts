/**
 * Commands as Effect Schema classes.
 *
 * Construct with `XCommand.make({ ... })` — `type` and `commandId` default
 * automatically (override `commandId` when you need idempotent / saga ids).
 */

import { randomUUID } from "node:crypto"

import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import { CommandId, ListId, TodoId } from "./ids.ts"

/** Fresh command id for constructor defaults. */
const newCommandId = (): CommandId => `cmd_${randomUUID()}` as CommandId

/**
 * `commandId` with a constructor default. Kept as plain String+brand pipe so
 * the Class field type stays `CommandId` (not `unknown`).
 */
const CommandIdField = Schema.String.pipe(
  Schema.brand("CommandId"),
  Schema.withConstructorDefault(Effect.sync(newCommandId)),
)

/**
 * Create a todo list.
 *
 * @example
 * ```ts
 * CreateListCommand.make({ listId: "list_1", title: "Groceries" })
 * ```
 */
export class CreateListCommand extends Schema.Class<CreateListCommand>("CreateListCommand")({
  type: Schema.tag("list.create"),
  commandId: CommandIdField,
  listId: ListId,
  title: Schema.String,
}) {}

/**
 * Add a todo to a list.
 *
 * @example
 * ```ts
 * AddTodoCommand.make({ listId: "list_1", todoId: "todo_1", text: "Milk" })
 * ```
 */
export class AddTodoCommand extends Schema.Class<AddTodoCommand>("AddTodoCommand")({
  type: Schema.tag("todo.add"),
  commandId: CommandIdField,
  listId: ListId,
  todoId: TodoId,
  text: Schema.String,
}) {}

/**
 * Mark a todo completed.
 *
 * @example
 * ```ts
 * CompleteTodoCommand.make({ listId: "list_1", todoId: "todo_1" })
 * ```
 */
export class CompleteTodoCommand extends Schema.Class<CompleteTodoCommand>("CompleteTodoCommand")({
  type: Schema.tag("todo.complete"),
  commandId: CommandIdField,
  listId: ListId,
  todoId: TodoId,
}) {}

/**
 * Rename a todo.
 *
 * @example
 * ```ts
 * RenameTodoCommand.make({ listId: "list_1", todoId: "todo_1", text: "Oat milk" })
 * ```
 */
export class RenameTodoCommand extends Schema.Class<RenameTodoCommand>("RenameTodoCommand")({
  type: Schema.tag("todo.rename"),
  commandId: CommandIdField,
  listId: ListId,
  todoId: TodoId,
  text: Schema.String,
}) {}

/**
 * Remove a todo from a list.
 *
 * @example
 * ```ts
 * RemoveTodoCommand.make({ listId: "list_1", todoId: "todo_1" })
 * ```
 */
export class RemoveTodoCommand extends Schema.Class<RemoveTodoCommand>("RemoveTodoCommand")({
  type: Schema.tag("todo.remove"),
  commandId: CommandIdField,
  listId: ListId,
  todoId: TodoId,
}) {}

/**
 * Archive a list (often produced by a saga when all todos are done).
 *
 * @example
 * ```ts
 * ArchiveListCommand.make({ listId: "list_1" })
 * ArchiveListCommand.make({
 *   listId: "list_1",
 *   commandId: "saga:archive:list_1:@12",
 * })
 * ```
 */
export class ArchiveListCommand extends Schema.Class<ArchiveListCommand>("ArchiveListCommand")({
  type: Schema.tag("list.archive"),
  commandId: CommandIdField,
  listId: ListId,
}) {}

/** Union of all command class instances (what the engine accepts). */
export type Command =
  | CreateListCommand
  | AddTodoCommand
  | CompleteTodoCommand
  | RenameTodoCommand
  | RemoveTodoCommand
  | ArchiveListCommand

/** Schema union for decoding unknown command payloads. */
export const CommandSchema = Schema.Union([
  CreateListCommand,
  AddTodoCommand,
  CompleteTodoCommand,
  RenameTodoCommand,
  RemoveTodoCommand,
  ArchiveListCommand,
])
