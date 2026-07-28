/**
 * Composed application layer — engine + in-memory persistence + Node crypto.
 */

import * as Layer from "effect/Layer"

import { EngineLive } from "./engine/Engine.ts"
import { CommandReceiptsLive } from "./persistence/CommandReceipts.ts"
import { EventStoreLive } from "./persistence/EventStore.ts"
import { CryptoLive } from "./runtime/CryptoLive.ts"

export const AppLayer = EngineLive.pipe(
  Layer.provide(EventStoreLive),
  Layer.provide(CommandReceiptsLive),
  Layer.provide(CryptoLive),
)
