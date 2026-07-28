/**
 * Read model helpers over Effect HashMap.
 */

import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"

import type { ListId, ReadModel, TodoList } from "./types.ts"

export const findList = (model: ReadModel, listId: ListId): Option.Option<TodoList> =>
  HashMap.get(model.lists, listId)

export const upsertList = (
  model: ReadModel,
  listId: ListId,
  list: TodoList,
): ReadModel => ({
  ...model,
  lists: HashMap.set(model.lists, listId, list),
})

export const updateList = (
  model: ReadModel,
  listId: ListId,
  f: (list: TodoList) => TodoList,
): ReadModel => {
  const current = HashMap.get(model.lists, listId)
  if (Option.isNone(current)) return model
  return upsertList(model, listId, f(current.value))
}
