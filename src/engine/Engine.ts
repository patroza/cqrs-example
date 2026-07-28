/**
 * Engine — CommandBus + projection core (event-sourced).
 *
 *   command → decide → append → project → DomainEvents + handlers + sagas
 */

import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"

import { DomainEvents } from "../cqrs/DomainEvents.ts"
import {
  AuditLog,
  runEventHandlers,
  UnhandledExceptionBus,
  type EventHandler,
  type UnhandledExceptionBusShape,
} from "../cqrs/eventHandlers.ts"
import { defaultSagas, runSagas, type Saga } from "../cqrs/sagas.ts"
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
  readonly dispatch: (command: Command) => Effect.Effect<DispatchResult, DispatchError>
  readonly getSnapshot: () => Effect.Effect<Snapshot>
  readonly getReadModel: () => Effect.Effect<ReadModel>
  readonly streamEvents: () => Stream.Stream<DomainEvent>
  readonly replayFrom: (
    afterSequence: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<DomainEvent>>
}

export class Engine extends Context.Service<Engine, EngineShape>()(
  "cqrs-example/engine/Engine",
) {
  static readonly layer = Layer.effect(
    Engine,
    Effect.gen(function* () {
      const eventStore = yield* EventStore
      const receipts = yield* CommandReceipts
      const domainEvents = yield* DomainEvents
      const auditLog = yield* AuditLog
      const unhandled = yield* UnhandledExceptionBus

      const eventHandlers: ReadonlyArray<EventHandler> = [auditLog.asHandler()]
      const sagas: ReadonlyArray<Saga> = defaultSagas

      const existing = yield* eventStore.readAll()
      const readModelRef = yield* Ref.make(
        existing.length === 0 ? emptyReadModel() : rebuildFromEvents(existing),
      )

      const commandQueue = yield* Queue.unbounded<Envelope>()

      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            const envelope = yield* Queue.take(commandQueue)
            yield* processEnvelope({
              envelope,
              eventStore,
              receipts,
              readModelRef,
              domainEvents,
              commandQueue,
              eventHandlers,
              sagas,
              unhandled,
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
        return toSnapshot(yield* Ref.get(readModelRef))
      })

      const getReadModel = Effect.fn("Engine.getReadModel")(function* () {
        return yield* Ref.get(readModelRef)
      })

      const streamEvents = () => domainEvents.subscribe

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
}

/** @deprecated Prefer `Engine.layer` */
export const EngineLive = Engine.layer

type Envelope = {
  readonly command: Command
  readonly result: Deferred.Deferred<DispatchResult, DispatchError>
}

const processEnvelope = Effect.fn("Engine.processEnvelope")(function* (args: {
  readonly envelope: Envelope
  readonly eventStore: EventStoreShape
  readonly receipts: CommandReceiptsShape
  readonly readModelRef: Ref.Ref<ReadModel>
  readonly domainEvents: DomainEvents["Service"]
  readonly commandQueue: Queue.Queue<Envelope>
  readonly eventHandlers: ReadonlyArray<EventHandler>
  readonly sagas: ReadonlyArray<Saga>
  readonly unhandled: UnhandledExceptionBusShape
}) {
  const {
    envelope,
    eventStore,
    receipts,
    readModelRef,
    domainEvents,
    commandQueue,
    eventHandlers,
    sagas,
    unhandled,
  } = args
  const { command, result } = envelope

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
    yield* domainEvents.publish(event)
    yield* runEventHandlers({ event, handlers: eventHandlers, unhandled })

    const followUps = runSagas(event, nextModel, sagas)
    for (const followUp of followUps) {
      const sagaResult = yield* Deferred.make<DispatchResult, DispatchError>()
      yield* Queue.offer(commandQueue, { command: followUp, result: sagaResult })
    }
  }

  yield* Deferred.succeed(result, { sequence: lastSequence })
})
