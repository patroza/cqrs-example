/**
 * DomainEvents — in-process EventBus (Effect PubSub service).
 *
 * Mirrors effect ai-docs PubSub pattern and Nest EventBus / T3 domain event stream.
 */

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Stream from "effect/Stream"

import type { DomainEvent } from "../domain/types.ts"

export interface DomainEventsShape {
  readonly publish: (event: DomainEvent) => Effect.Effect<void>
  readonly publishAll: (events: ReadonlyArray<DomainEvent>) => Effect.Effect<void>
  readonly subscribe: Stream.Stream<DomainEvent>
}

export class DomainEvents extends Context.Service<DomainEvents, DomainEventsShape>()(
  "cqrs-example/cqrs/DomainEvents",
) {
  static readonly layer = Layer.effect(
    DomainEvents,
    Effect.gen(function* () {
      const pubsub = yield* PubSub.unbounded<DomainEvent>()
      yield* Effect.addFinalizer(() => PubSub.shutdown(pubsub))

      const publish = Effect.fn("DomainEvents.publish")(function* (event: DomainEvent) {
        yield* PubSub.publish(pubsub, event)
      })

      const publishAll = Effect.fn("DomainEvents.publishAll")(function* (
        events: ReadonlyArray<DomainEvent>,
      ) {
        yield* PubSub.publishAll(pubsub, events)
      })

      return DomainEvents.of({
        publish,
        publishAll,
        subscribe: Stream.fromPubSub(pubsub),
      })
    }),
  )
}
