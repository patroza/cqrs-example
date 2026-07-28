/**
 * Event handlers — Nest `@EventsHandler` analogue.
 *
 * These are **side-effect** subscribers. They are NOT the projector.
 * Projector = pure fold into read model.
 * Event handler = audit log, metrics, notifications, …
 *
 * Nest note: handler errors do not surface on the command path; we record
 * them on UnhandledExceptionBus instead of failing the dispatch that
 * produced the event.
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
  /** Built-in Nest-style event handler that only records events. */
  readonly asHandler: () => EventHandler
}

export class AuditLog extends Context.Service<AuditLog, AuditLogShape>()(
  "cqrs-example/cqrs/AuditLog",
) {}

export const AuditLogLive = Layer.effect(
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
        const entry: AuditEntry = {
          sequence: event.sequence,
          eventType: event.type,
          aggregateId: event.aggregateId,
          recordedAt: event.occurredAt,
        }
        yield* Ref.update(entriesRef, (xs) => [...xs, entry])
      }),
    })

    return AuditLog.of({ entries, clear, asHandler })
  }),
)

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

/**
 * Nest `UnhandledExceptionBus` analogue for async event-handler failures.
 */
export class UnhandledExceptionBus extends Context.Service<
  UnhandledExceptionBus,
  UnhandledExceptionBusShape
>()("cqrs-example/cqrs/UnhandledExceptionBus") {}

export const UnhandledExceptionBusLive = Layer.effect(
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

/**
 * Run all event handlers for one committed event.
 * Failures are swallowed into UnhandledExceptionBus (Nest behaviour).
 */
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
