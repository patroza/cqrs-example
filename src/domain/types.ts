/**
 * Domain contracts for a tiny todo CQRS/ES sample.
 *
 * Mirrors T3 orchestration ideas:
 * - Commands = intent (Schema classes — see commands.ts)
 * - Events = durable facts (source of truth)
 * - Read model = projection of events
 * - sequence = global monotonic cursor for catch-up / replay
 */

import * as HashMap from "effect/HashMap"

import type { CommandId, EventId, ListId, TodoId } from "./ids.ts"

export * from "./ids.ts"
export * from "./commands.ts"

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
 * Lists live in an Effect `HashMap` (immutable, structural equality).
 */
export type ReadModel = {
  /** Last applied global event sequence (0 = empty). */
  readonly snapshotSequence: number
  readonly lists: HashMap.HashMap<ListId, TodoList>
}

/** Point-in-time view a client would hydrate from (like T3 shell/thread snapshots). */
export type Snapshot = {
  readonly snapshotSequence: number
  readonly lists: ReadonlyArray<TodoList>
}

export const emptyReadModel = (): ReadModel => ({
  snapshotSequence: 0,
  lists: HashMap.empty(),
})

export const toSnapshot = (model: ReadModel): Snapshot => ({
  snapshotSequence: model.snapshotSequence,
  lists: Array.from(HashMap.values(model.lists)),
})
