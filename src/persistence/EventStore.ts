/**
 * In-memory event store.
 *
 * Same contract shape as T3's OrchestrationEventStore:
 * - append assigns a global sequence
 * - readFromSequence(after) supports catch-up / projection bootstrap
 */

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"

import type { DomainEvent, UnsequencedEvent } from "../domain/types.ts"

export interface EventStoreShape {
  readonly append: (event: UnsequencedEvent) => Effect.Effect<DomainEvent>
  readonly readFromSequence: (
    sequenceExclusive: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<DomainEvent>>
  readonly readAll: () => Effect.Effect<ReadonlyArray<DomainEvent>>
  readonly latestSequence: () => Effect.Effect<number>
}

export class EventStore extends Context.Service<EventStore, EventStoreShape>()(
  "cqrs-example/persistence/EventStore",
) {
  static readonly layer = Layer.effect(
    EventStore,
    Effect.gen(function* () {
      const stateRef = yield* Ref.make<{
        readonly nextSequence: number
        readonly events: ReadonlyArray<DomainEvent>
      }>({ nextSequence: 1, events: [] })

      const append = Effect.fn("EventStore.append")(function* (event: UnsequencedEvent) {
        return yield* Ref.modify(stateRef, (state) => {
          const sequenced = {
            ...event,
            sequence: state.nextSequence,
          } as DomainEvent
          return [
            sequenced,
            {
              nextSequence: state.nextSequence + 1,
              events: [...state.events, sequenced],
            },
          ] as const
        })
      })

      const readFromSequence = Effect.fn("EventStore.readFromSequence")(function* (
        sequenceExclusive: number,
        limit = 1_000,
      ) {
        const { events } = yield* Ref.get(stateRef)
        return events
          .filter((e) => e.sequence > sequenceExclusive)
          .slice(0, Math.max(0, limit))
      })

      const readAll = Effect.fn("EventStore.readAll")(function* () {
        const { events } = yield* Ref.get(stateRef)
        return events
      })

      const latestSequence = Effect.fn("EventStore.latestSequence")(function* () {
        const { events } = yield* Ref.get(stateRef)
        return events.at(-1)?.sequence ?? 0
      })

      return EventStore.of({
        append,
        readFromSequence,
        readAll,
        latestSequence,
      })
    }),
  )
}

/** @deprecated Prefer `EventStore.layer` */
export const EventStoreLive = EventStore.layer
