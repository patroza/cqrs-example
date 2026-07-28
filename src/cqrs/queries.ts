/**
 * Query side — Nest `Query` / `QueryHandler` analogue.
 *
 * Reads never go through the command worker. They only look at the
 * projected read model (same idea as Nest QueryBus → QueryHandler → repo).
 */

import * as Schema from "effect/Schema"

import type { ListId, ReadModel, Snapshot, TodoList } from "../domain/types.ts"
import { toSnapshot } from "../domain/types.ts"

export type GetSnapshotQuery = {
  readonly type: "snapshot.get"
}

export type GetListQuery = {
  readonly type: "list.get"
  readonly listId: ListId
}

export type ListListsQuery = {
  readonly type: "list.list"
  /** When true, omit archived lists. */
  readonly activeOnly?: boolean
}

export type Query = GetSnapshotQuery | GetListQuery | ListListsQuery

export type GetListResult = {
  readonly found: true
  readonly list: TodoList
} | {
  readonly found: false
}

export type QueryResult =
  | { readonly type: "snapshot.get"; readonly value: Snapshot }
  | { readonly type: "list.get"; readonly value: GetListResult }
  | { readonly type: "list.list"; readonly value: ReadonlyArray<TodoList> }

export class QueryNotFoundError extends Schema.TaggedErrorClass<QueryNotFoundError>()(
  "QueryNotFoundError",
  {
    queryType: Schema.String,
  },
) {}

/**
 * Pure query handler (Nest `IQueryHandler.execute`).
 * Kept pure so tests can run without the engine.
 */
export const handleQuery = (query: Query, model: ReadModel): QueryResult => {
  switch (query.type) {
    case "snapshot.get":
      return { type: "snapshot.get", value: toSnapshot(model) }
    case "list.get": {
      const list = model.lists.get(query.listId)
      return {
        type: "list.get",
        value: list ? { found: true, list } : { found: false },
      }
    }
    case "list.list": {
      let lists = Array.from(model.lists.values())
      if (query.activeOnly) {
        lists = lists.filter((list) => !list.archived)
      }
      return { type: "list.list", value: lists }
    }
  }
}
