/**
 * Command receipt store — idempotent dispatch by commandId.
 */

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"

import type { CommandId } from "../domain/types.ts"

export type CommandReceipt =
  | {
      readonly commandId: CommandId
      readonly status: "accepted"
      readonly resultSequence: number
    }
  | {
      readonly commandId: CommandId
      readonly status: "rejected"
      readonly detail: string
    }

export interface CommandReceiptsShape {
  readonly get: (commandId: CommandId) => Effect.Effect<Option.Option<CommandReceipt>>
  readonly put: (receipt: CommandReceipt) => Effect.Effect<void>
}

export class CommandReceipts extends Context.Service<CommandReceipts, CommandReceiptsShape>()(
  "cqrs-example/persistence/CommandReceipts",
) {
  static readonly layer = Layer.effect(
    CommandReceipts,
    Effect.gen(function* () {
      const mapRef = yield* Ref.make(new Map<CommandId, CommandReceipt>())

      const get = Effect.fn("CommandReceipts.get")(function* (commandId: CommandId) {
        const map = yield* Ref.get(mapRef)
        return Option.fromNullishOr(map.get(commandId))
      })

      const put = Effect.fn("CommandReceipts.put")(function* (receipt: CommandReceipt) {
        yield* Ref.update(mapRef, (map) => {
          const next = new Map(map)
          next.set(receipt.commandId, receipt)
          return next
        })
      })

      return CommandReceipts.of({ get, put })
    }),
  )
}

/** @deprecated Prefer `CommandReceipts.layer` */
export const CommandReceiptsLive = CommandReceipts.layer
