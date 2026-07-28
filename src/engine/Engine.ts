/**
 * Engine — serialize commands, decide, append, project, publish.
 *
 * Simplified counterpart of T3's OrchestrationEngine:
 *   command → decide → append events → project read model → publish live
 *
 * Command-side read model is kept in memory and advanced as events commit.
 * On boot we rebuild from the event store (classic ES). T3 seeds from
 * projection tables + catch-up; either is valid.
 */

import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"

import { decide } from "../domain/decider.ts"
import {
  CommandInvariantError,
  CommandPreviouslyRejectedError,
} from "../domain/errors.ts"
import { projectEvent, rebuildFromEvents } from "../domain/projector.ts"
import type { Command, DomainEvent, ReadModel, Snapshot } from "../domain/types.ts"
import { emptyReadModel, toSnapshot } from "../domain/types.ts"
import { CommandReceipts, type CommandReceiptsShape } from "../persistence/CommandReceipts.ts"
import { EventStore, type EventStoreShape } from "../persistence/EventStore.ts"

export type DispatchResult = {
  readonly sequence: number
}

export type DispatchError = CommandInvariantError | CommandPreviouslyRejectedError

export interface EngineShape {
  /** Submit a command; returns last committed sequence on success. */
  readonly dispatch: (command: Command) => Effect.Effect<DispatchResult, DispatchError>
  /** Materialized snapshot for client hydration. */
  readonly getSnapshot: () => Effect.Effect<Snapshot>
  /** Full command-side read model (for tests / demos). */
  readonly getReadModel: () => Effect.Effect<ReadModel>
  /** Live domain event stream (after subscribe). */
  readonly streamEvents: () => Stream.Stream<DomainEvent>
  /**
   * Replay events after a sequence cursor (catch-up).
   * Same idea as orchestration.replayEvents / subscribe catch-up.
   */
  readonly replayFrom: (
    afterSequence: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<DomainEvent>>
}

export class Engine extends Context.Service<Engine, EngineShape>()(
  "cqrs-example/engine/Engine",
) {}

type Envelope = {
  readonly command: Command
  readonly result: Deferred.Deferred<DispatchResult, DispatchError>
}

export const EngineLive = Layer.effect(
  Engine,
  Effect.gen(function* () {
    const eventStore = yield* EventStore
    const receipts = yield* CommandReceipts

    // Boot: rebuild command-side model from the event log (full classic ES).
    const existing = yield* eventStore.readAll()
    const readModelRef = yield* Ref.make(
      existing.length === 0 ? emptyReadModel() : rebuildFromEvents(existing),
    )

    const eventPubSub = yield* PubSub.unbounded<DomainEvent>()
    const commandQueue = yield* Queue.unbounded<Envelope>()

    // Single worker serializes all commands — avoids concurrent decide races.
    yield* Effect.forkScoped(
      Effect.forever(
        Effect.gen(function* () {
          const envelope = yield* Queue.take(commandQueue)
          yield* processEnvelope({
            envelope,
            eventStore,
            receipts,
            readModelRef,
            eventPubSub,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("command worker failed").pipe(
                Effect.annotateLogs({ cause: String(cause) }),
              ),
            ),
          )
        }),
      ),
    )

    const dispatch = Effect.fn("Engine.dispatch")(function* (command: Command) {
      const result = yield* Deferred.make<DispatchResult, DispatchError>()
      yield* Queue.offer(commandQueue, { command, result })
      return yield* Deferred.await(result)
    })

    const getSnapshot = Effect.fn("Engine.getSnapshot")(function* () {
      const model = yield* Ref.get(readModelRef)
      return toSnapshot(model)
    })

    const getReadModel = Effect.fn("Engine.getReadModel")(function* () {
      return yield* Ref.get(readModelRef)
    })

    const streamEvents = () => Stream.fromPubSub(eventPubSub)

    const replayFrom = Effect.fn("Engine.replayFrom")(function* (
      afterSequence: number,
      limit?: number,
    ) {
      return yield* eventStore.readFromSequence(afterSequence, limit)
    })

    return Engine.of({
      dispatch,
      getSnapshot,
      getReadModel,
      streamEvents,
      replayFrom,
    })
  }),
)

const processEnvelope = Effect.fn("Engine.processEnvelope")(function* (args: {
  readonly envelope: Envelope
  readonly eventStore: EventStoreShape
  readonly receipts: CommandReceiptsShape
  readonly readModelRef: Ref.Ref<ReadModel>
  readonly eventPubSub: PubSub.PubSub<DomainEvent>
}) {
  const { envelope, eventStore, receipts, readModelRef, eventPubSub } = args
  const { command, result } = envelope

  // Idempotency: same commandId → same outcome.
  const existing = yield* receipts.get(command.commandId)
  if (Option.isSome(existing)) {
    const receipt = existing.value
    if (receipt.status === "accepted") {
      yield* Deferred.succeed(result, { sequence: receipt.resultSequence })
      return
    }
    yield* Deferred.fail(
      result,
      new CommandPreviouslyRejectedError({
        commandId: command.commandId,
        detail: receipt.detail,
      }),
    )
    return
  }

  const readModel = yield* Ref.get(readModelRef)

  const decided = yield* Effect.result(decide({ command, readModel }))

  if (Result.isFailure(decided)) {
    const error = decided.failure
    yield* receipts.put({
      commandId: command.commandId,
      status: "rejected",
      detail: error.detail,
    })
    yield* Deferred.fail(result, error)
    return
  }

  let nextModel = readModel
  let lastSequence = readModel.snapshotSequence
  const committed: DomainEvent[] = []

  for (const unsequenced of decided.success) {
    const saved = yield* eventStore.append(unsequenced)
    nextModel = projectEvent(nextModel, saved)
    committed.push(saved)
    lastSequence = saved.sequence
  }

  yield* Ref.set(readModelRef, nextModel)
  yield* receipts.put({
    commandId: command.commandId,
    status: "accepted",
    resultSequence: lastSequence,
  })

  for (const event of committed) {
    yield* PubSub.publish(eventPubSub, event)
  }

  yield* Deferred.succeed(result, { sequence: lastSequence })
})
