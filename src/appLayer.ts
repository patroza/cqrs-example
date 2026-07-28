/**
 * Composed application layer — engine + Nest-like CQRS surfaces.
 *
 * `provideMerge` keeps shared infra (AuditLog, EventStore, …) as the same
 * instances Engine uses and tests can inject.
 */

import * as Layer from "effect/Layer"

import { AuditLogLive, UnhandledExceptionBusLive } from "./cqrs/eventHandlers.ts"
import { QueryBusLive } from "./cqrs/QueryBus.ts"
import { EngineLive } from "./engine/Engine.ts"
import { CommandReceiptsLive } from "./persistence/CommandReceipts.ts"
import { EventStoreLive } from "./persistence/EventStore.ts"
import { CryptoLive } from "./runtime/CryptoLive.ts"

const EngineStack = EngineLive.pipe(
  Layer.provideMerge(EventStoreLive),
  Layer.provideMerge(CommandReceiptsLive),
  Layer.provideMerge(AuditLogLive),
  Layer.provideMerge(UnhandledExceptionBusLive),
  Layer.provideMerge(CryptoLive),
)

/** Full app: QueryBus + Engine + shared infra. */
export const AppLayer = QueryBusLive.pipe(Layer.provideMerge(EngineStack))
