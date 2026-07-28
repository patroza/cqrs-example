/**
 * Sagas — Nest `@Saga()` analogue: event → follow-up commands.
 */

import * as Option from "effect/Option"

import { ArchiveListCommand } from "../domain/commands.ts"
import { CommandId } from "../domain/ids.ts"
import { findList } from "../domain/readModel.ts"
import type { Command, DomainEvent, ReadModel } from "../domain/types.ts"

export type Saga = {
  readonly name: string
  readonly react: (
    event: DomainEvent,
    readModel: ReadModel,
  ) => ReadonlyArray<Command>
}

/**
 * When every todo on a list is completed, archive the list.
 */
export const archiveWhenAllCompleteSaga: Saga = {
  name: "ArchiveWhenAllCompleteSaga",
  react: (event, readModel) => {
    if (event.type !== "todo.completed" && event.type !== "todo.removed") {
      return []
    }

    const listId = event.payload.listId
    const list = findList(readModel, listId)
    if (Option.isNone(list) || list.value.archived) return []
    if (list.value.todos.length === 0) return []
    if (!list.value.todos.every((todo) => todo.completed)) return []

    return [
      ArchiveListCommand.make({
        listId,
        commandId: CommandId.make(`saga:archive:${listId}:@${event.sequence}`),
      }),
    ]
  },
}

export const defaultSagas: ReadonlyArray<Saga> = [archiveWhenAllCompleteSaga]

export const runSagas = (
  event: DomainEvent,
  readModel: ReadModel,
  sagas: ReadonlyArray<Saga> = defaultSagas,
): ReadonlyArray<Command> => sagas.flatMap((saga) => saga.react(event, readModel))
