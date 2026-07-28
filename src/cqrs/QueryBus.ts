/**
 * QueryBus — Nest `QueryBus.execute(query)` analogue.
 */

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

import { Engine } from "../engine/Engine.ts"
import { handleQuery, type Query, type QueryResult } from "./queries.ts"

export interface QueryBusShape {
  readonly execute: (query: Query) => Effect.Effect<QueryResult>
}

export class QueryBus extends Context.Service<QueryBus, QueryBusShape>()(
  "cqrs-example/cqrs/QueryBus",
) {}

export const QueryBusLive = Layer.effect(
  QueryBus,
  Effect.gen(function* () {
    const engine = yield* Engine

    const execute = Effect.fn("QueryBus.execute")(function* (query: Query) {
      const model = yield* engine.getReadModel()
      return handleQuery(query, model)
    })

    return QueryBus.of({ execute })
  }),
)
