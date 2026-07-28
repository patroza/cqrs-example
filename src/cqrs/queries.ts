/**
 * Query side — Nest `Query` / `QueryHandler` analogue.
 */

import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import { findList } from "../domain/readModel.ts"
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
  readonly activeOnly?: boolean
}

export type Query = GetSnapshotQuery | GetListQuery | ListListsQuery

export type GetListResult =
  | {
      readonly found: true
      readonly list: TodoList
    }
  | {
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

/** Pure query handler (Nest `IQueryHandler.execute`). */
export const handleQuery = (query: Query, model: ReadModel): QueryResult => {
  switch (query.type) {
    case "snapshot.get":
      return { type: "snapshot.get", value: toSnapshot(model) }
    case "list.get": {
      const list = findList(model, query.listId)
      return {
        type: "list.get",
        value: Option.isSome(list)
          ? { found: true, list: list.value }
          : { found: false },
      }
    }
    case "list.list": {
      let lists = Array.from(HashMap.values(model.lists))
      if (query.activeOnly) {
        lists = lists.filter((list) => !list.archived)
      }
      return { type: "list.list", value: lists }
    }
  }
}
