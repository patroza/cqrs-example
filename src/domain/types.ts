/**
 * Domain contracts for a tiny todo CQRS/ES sample.
 *
 * Mirrors T3 orchestration ideas:
 * - Commands = intent
 * - Events = durable facts (source of truth)
 * - Read model = projection of events
 * - sequence = global monotonic cursor for catch-up / replay
 */

import * as Schema from "effect/Schema"

// ---------------------------------------------------------------------------
// IDs
// ---------------------------------------------------------------------------

export const ListId = Schema.String.pipe(Schema.brand("ListId"))
export type ListId = typeof ListId.Type

export const TodoId = Schema.String.pipe(Schema.brand("TodoId"))
export type TodoId = typeof TodoId.Type

export const CommandId = Schema.String.pipe(Schema.brand("CommandId"))
export type CommandId = typeof CommandId.Type

export const EventId = Schema.String.pipe(Schema.brand("EventId"))
export type EventId = typeof EventId.Type

// ---------------------------------------------------------------------------
// Commands (intent)
// ---------------------------------------------------------------------------

export type CreateListCommand = {
  readonly type: "list.create"
  readonly commandId: CommandId
  readonly listId: ListId
  readonly title: string
}

export type AddTodoCommand = {
  readonly type: "todo.add"
  readonly commandId: CommandId
  readonly listId: ListId
  readonly todoId: TodoId
  readonly text: string
}

export type CompleteTodoCommand = {
  readonly type: "todo.complete"
  readonly commandId: CommandId
  readonly listId: ListId
  readonly todoId: TodoId
}

export type RenameTodoCommand = {
  readonly type: "todo.rename"
  readonly commandId: CommandId
  readonly listId: ListId
  readonly todoId: TodoId
  readonly text: string
}

export type RemoveTodoCommand = {
  readonly type: "todo.remove"
  readonly commandId: CommandId
  readonly listId: ListId
  readonly todoId: TodoId
}

/** Nest-style process-manager outcome: archive a list once work is done. */
export type ArchiveListCommand = {
  readonly type: "list.archive"
  readonly commandId: CommandId
  readonly listId: ListId
}

export type Command =
  | CreateListCommand
  | AddTodoCommand
  | CompleteTodoCommand
  | RenameTodoCommand
  | RemoveTodoCommand
  | ArchiveListCommand

// ---------------------------------------------------------------------------
// Domain events (facts) — source of truth once persisted with a sequence
// ---------------------------------------------------------------------------

export type ListCreatedPayload = {
  readonly listId: ListId
  readonly title: string
}

export type TodoAddedPayload = {
  readonly listId: ListId
  readonly todoId: TodoId
  readonly text: string
}

export type TodoCompletedPayload = {
  readonly listId: ListId
  readonly todoId: TodoId
}

export type TodoRenamedPayload = {
  readonly listId: ListId
  readonly todoId: TodoId
  readonly text: string
}

export type TodoRemovedPayload = {
  readonly listId: ListId
  readonly todoId: TodoId
}

export type ListArchivedPayload = {
  readonly listId: ListId
}

/**
 * Envelope matching the spirit of T3's OrchestrationEvent:
 * sequence is assigned by the event store on append.
 */
export type DomainEvent =
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "list.created"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: ListCreatedPayload
    }
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "todo.added"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: TodoAddedPayload
    }
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "todo.completed"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: TodoCompletedPayload
    }
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "todo.renamed"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: TodoRenamedPayload
    }
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "todo.removed"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: TodoRemovedPayload
    }
  | {
      readonly sequence: number
      readonly eventId: EventId
      readonly type: "list.archived"
      readonly aggregateId: ListId
      readonly commandId: CommandId
      readonly occurredAt: string
      readonly payload: ListArchivedPayload
    }

/** Event before the store assigns sequence. */
export type UnsequencedEvent = Omit<DomainEvent, "sequence">

// ---------------------------------------------------------------------------
// Read model (projection)
// ---------------------------------------------------------------------------

export type Todo = {
  readonly id: TodoId
  readonly text: string
  readonly completed: boolean
}

export type TodoList = {
  readonly id: ListId
  readonly title: string
  readonly todos: ReadonlyArray<Todo>
  /** Set by `list.archive` (often via a Nest-style saga). */
  readonly archived: boolean
}

/**
 * Command-side + query-side read model for this sample.
 * In T3 these are split (in-memory CommandReadModel + SQL projections),
 * but the fold is the same idea: pure event → state.
 */
export type ReadModel = {
  /** Last applied global event sequence (0 = empty). */
  readonly snapshotSequence: number
  readonly lists: ReadonlyMap<ListId, TodoList>
}

export const emptyReadModel = (): ReadModel => ({
  snapshotSequence: 0,
  lists: new Map(),
})

/** Point-in-time view a client would hydrate from (like T3 shell/thread snapshots). */
export type Snapshot = {
  readonly snapshotSequence: number
  readonly lists: ReadonlyArray<TodoList>
}

export const toSnapshot = (model: ReadModel): Snapshot => ({
  snapshotSequence: model.snapshotSequence,
  lists: Array.from(model.lists.values()),
})
