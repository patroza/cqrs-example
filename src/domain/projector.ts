/**
 * Projector — pure event → read model fold.
 *
 * Same role as T3's `apps/server/src/orchestration/projector.ts`.
 * Events are applied in sequence order; snapshotSequence advances to event.sequence.
 */

import type { DomainEvent, ReadModel, TodoList } from "./types.ts"
import { emptyReadModel } from "./types.ts"

const updateList = (
  model: ReadModel,
  listId: TodoList["id"],
  f: (list: TodoList) => TodoList,
): ReadModel => {
  const current = model.lists.get(listId)
  if (!current) return model
  const lists = new Map(model.lists)
  lists.set(listId, f(current))
  return { ...model, lists }
}

/**
 * Apply a single domain event. Idempotent w.r.t. sequence: events with
 * sequence <= model.snapshotSequence are ignored (client catch-up safety).
 */
export const projectEvent = (model: ReadModel, event: DomainEvent): ReadModel => {
  if (event.sequence <= model.snapshotSequence) {
    return model
  }

  const withSequence = (next: Omit<ReadModel, "snapshotSequence">): ReadModel => ({
    ...next,
    snapshotSequence: event.sequence,
  })

  switch (event.type) {
    case "list.created": {
      const lists = new Map(model.lists)
      lists.set(event.payload.listId, {
        id: event.payload.listId,
        title: event.payload.title,
        todos: [],
      })
      return withSequence({ lists })
    }

    case "todo.added":
      return withSequence(
        updateList(model, event.payload.listId, (list) => ({
          ...list,
          todos: [
            ...list.todos,
            {
              id: event.payload.todoId,
              text: event.payload.text,
              completed: false,
            },
          ],
        })),
      )

    case "todo.completed":
      return withSequence(
        updateList(model, event.payload.listId, (list) => ({
          ...list,
          todos: list.todos.map((todo) =>
            todo.id === event.payload.todoId ? { ...todo, completed: true } : todo,
          ),
        })),
      )

    case "todo.renamed":
      return withSequence(
        updateList(model, event.payload.listId, (list) => ({
          ...list,
          todos: list.todos.map((todo) =>
            todo.id === event.payload.todoId
              ? { ...todo, text: event.payload.text }
              : todo,
          ),
        })),
      )

    case "todo.removed":
      return withSequence(
        updateList(model, event.payload.listId, (list) => ({
          ...list,
          todos: list.todos.filter((todo) => todo.id !== event.payload.todoId),
        })),
      )
  }
}

/** Fold a batch of events onto a base model (bootstrap / catch-up). */
export const projectEvents = (
  model: ReadModel,
  events: ReadonlyArray<DomainEvent>,
): ReadModel => events.reduce(projectEvent, model)

/** Rebuild state purely from the event log (classic ES). */
export const rebuildFromEvents = (events: ReadonlyArray<DomainEvent>): ReadModel =>
  projectEvents(emptyReadModel(), events)
