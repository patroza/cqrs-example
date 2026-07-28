/**
 * Event handlers — Nest `@EventsHandler` analogue (side effects ≠ projector).
 */

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"

import type { DomainEvent } from "../domain/types.ts"

export type AuditEntry = {
  readonly sequence: number
  readonly eventType: DomainEvent["type"]
  readonly aggregateId: string
  readonly recordedAt: string
}

export type EventHandler = {
  readonly name: string
  readonly handle: (event: DomainEvent) => Effect.Effect<void>
}

export interface AuditLogShape {
  readonly entries: () => Effect.Effect<ReadonlyArray<AuditEntry>>
  readonly clear: () => Effect.Effect<void>
  readonly asHandler: () => EventHandler
}

export class AuditLog extends Context.Service<AuditLog, AuditLogShape>()(
  "cqrs-example/cqrs/AuditLog",
) {
  static readonly layer = Layer.effect(
    AuditLog,
    Effect.gen(function* () {
      const entriesRef = yield* Ref.make<ReadonlyArray<AuditEntry>>([])

      const entries = Effect.fn("AuditLog.entries")(function* () {
        return yield* Ref.get(entriesRef)
      })

      const clear = Effect.fn("AuditLog.clear")(function* () {
        yield* Ref.set(entriesRef, [])
      })

      const asHandler = (): EventHandler => ({
        name: "AuditLogHandler",
        handle: Effect.fn("AuditLogHandler.handle")(function* (event: DomainEvent) {
          yield* Ref.update(entriesRef, (xs) => [
            ...xs,
            {
              sequence: event.sequence,
              eventType: event.type,
              aggregateId: event.aggregateId,
              recordedAt: event.occurredAt,
            },
          ])
        }),
      })

      return AuditLog.of({ entries, clear, asHandler })
    }),
  )
}

/** @deprecated Prefer `AuditLog.layer` */
export const AuditLogLive = AuditLog.layer

export type UnhandledExceptionInfo = {
  readonly handlerName: string
  readonly eventType: DomainEvent["type"]
  readonly sequence: number
  readonly message: string
}

export interface UnhandledExceptionBusShape {
  readonly publish: (info: UnhandledExceptionInfo) => Effect.Effect<void>
  readonly drain: () => Effect.Effect<ReadonlyArray<UnhandledExceptionInfo>>
}

export class UnhandledExceptionBus extends Context.Service<
  UnhandledExceptionBus,
  UnhandledExceptionBusShape
>()("cqrs-example/cqrs/UnhandledExceptionBus") {
  static readonly layer = Layer.effect(
    UnhandledExceptionBus,
    Effect.gen(function* () {
      const ref = yield* Ref.make<ReadonlyArray<UnhandledExceptionInfo>>([])

      const publish = Effect.fn("UnhandledExceptionBus.publish")(function* (
        info: UnhandledExceptionInfo,
      ) {
        yield* Ref.update(ref, (xs) => [...xs, info])
      })

      const drain = Effect.fn("UnhandledExceptionBus.drain")(function* () {
        return yield* Ref.getAndSet(ref, [])
      })

      return UnhandledExceptionBus.of({ publish, drain })
    }),
  )
}

/** @deprecated Prefer `UnhandledExceptionBus.layer` */
export const UnhandledExceptionBusLive = UnhandledExceptionBus.layer

export const runEventHandlers = Effect.fn("runEventHandlers")(function* (args: {
  readonly event: DomainEvent
  readonly handlers: ReadonlyArray<EventHandler>
  readonly unhandled: UnhandledExceptionBusShape
}) {
  const { event, handlers, unhandled } = args
  for (const handler of handlers) {
    yield* handler.handle(event).pipe(
      Effect.catchCause((cause) =>
        unhandled.publish({
          handlerName: handler.name,
          eventType: event.type,
          sequence: event.sequence,
          message: String(cause),
        }),
      ),
    )
  }
})
