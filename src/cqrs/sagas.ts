/**
 * Sagas — Nest `@Saga()` analogue: event stream → new commands.
 *
 * Unlike the projector (pure state fold) and event handlers (side effects),
 * a saga **reacts** by dispatching follow-up commands onto the CommandBus.
 *
 * Nest runs this asynchronously via RxJS Observables. We return commands
 * after each committed event and enqueue them (fire-and-forget) so the
 * single command worker never deadlocks awaiting itself.
 */

import type { Command, CommandId, DomainEvent, ReadModel } from "../domain/types.ts"

export type Saga = {
  readonly name: string
  /**
   * Inspect a committed event + post-projection read model.
   * Return zero or more commands to enqueue.
   */
  readonly react: (
    event: DomainEvent,
    readModel: ReadModel,
  ) => ReadonlyArray<Command>
}

/**
 * When every todo on a list is completed, archive the list.
 * Mirrors Nest's "HeroKilledDragon → DropAncientItem" process manager.
 */
export const archiveWhenAllCompleteSaga: Saga = {
  name: "ArchiveWhenAllCompleteSaga",
  react: (event, readModel) => {
    if (event.type !== "todo.completed" && event.type !== "todo.removed") {
      return []
    }

    const listId = event.payload.listId
    const list = readModel.lists.get(listId)
    if (!list || list.archived) return []
    if (list.todos.length === 0) return []
    if (!list.todos.every((todo) => todo.completed)) return []

    // Deterministic commandId → idempotent if the saga re-fires.
    const commandId = `saga:archive:${listId}:@${event.sequence}` as CommandId
    return [
      {
        type: "list.archive",
        commandId,
        listId,
      },
    ]
  },
}

export const defaultSagas: ReadonlyArray<Saga> = [archiveWhenAllCompleteSaga]

export const runSagas = (
  event: DomainEvent,
  readModel: ReadModel,
  sagas: ReadonlyArray<Saga> = defaultSagas,
): ReadonlyArray<Command> => sagas.flatMap((saga) => saga.react(event, readModel))
