/**
 * Composed application layer — Effect idiomatic service.layer composition.
 */

import { NodeCrypto } from "@effect/platform-node"
import * as Layer from "effect/Layer"

import { DomainEvents } from "./cqrs/DomainEvents.ts"
import { AuditLog, UnhandledExceptionBus } from "./cqrs/eventHandlers.ts"
import { QueryBus } from "./cqrs/QueryBus.ts"
import { Engine } from "./engine/Engine.ts"
import { CommandReceipts } from "./persistence/CommandReceipts.ts"
import { EventStore } from "./persistence/EventStore.ts"

const Shared = Layer.mergeAll(
  EventStore.layer,
  CommandReceipts.layer,
  DomainEvents.layer,
  AuditLog.layer,
  UnhandledExceptionBus.layer,
  NodeCrypto.layer,
)

const EngineLayer = Engine.layer.pipe(Layer.provide(Shared))
const QueryLayer = QueryBus.layer.pipe(Layer.provide(EngineLayer))

/** Full app: QueryBus + Engine + shared infra (same instances). */
export const AppLayer = Layer.mergeAll(Shared, EngineLayer, QueryLayer)
