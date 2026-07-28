/**
 * Client catch-up — snapshot + replay + live, sequence-deduped.
 */

import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"

import { projectEvent, projectEvents } from "../domain/projector.ts"
import type { DomainEvent, ReadModel, Snapshot } from "../domain/types.ts"
import { emptyReadModel } from "../domain/types.ts"
import type { Engine } from "../engine/Engine.ts"

/** How many events behind still triggers replay vs full snapshot resync. */
export const REPLAY_LIMIT = 1_000

export type CatchUpMode =
  | { readonly kind: "snapshot" }
  | { readonly kind: "replay"; readonly eventCount: number }
  | { readonly kind: "live" }

export type ClientState = {
  readonly model: ReadModel
  readonly lastMode: CatchUpMode
}

/**
 * Hydrate client state from the engine, then fold live events forever.
 */
export const subscribeWithCatchUp = Effect.fn("client.subscribeWithCatchUp")(function* (args: {
  readonly engine: Engine["Service"]
  readonly afterSequence?: number
}) {
  const { engine } = args
  const afterSequence = args.afterSequence ?? 0

  const liveQueue = yield* Queue.unbounded<DomainEvent>()
  yield* Stream.runForEach(engine.streamEvents(), (event) => Queue.offer(liveQueue, event)).pipe(
    Effect.forkScoped,
  )

  const snapshot = yield* engine.getSnapshot()
  const gap = snapshot.snapshotSequence - afterSequence

  let model: ReadModel
  let lastMode: CatchUpMode

  if (afterSequence <= 0 || gap < 0 || gap > REPLAY_LIMIT) {
    model = snapshotToReadModel(snapshot)
    lastMode = { kind: "snapshot" }
  } else {
    const localBase = yield* loadLocalBase(engine, afterSequence)
    if (localBase === null) {
      model = snapshotToReadModel(snapshot)
      lastMode = { kind: "snapshot" }
    } else {
      const events = yield* engine.replayFrom(afterSequence, gap)
      model = projectEvents(localBase, events)
      lastMode = { kind: "replay", eventCount: events.length }
    }
  }

  const stateRef = yield* Ref.make<ClientState>({ model, lastMode })
  const updates = yield* Queue.unbounded<ClientState>()
  yield* Queue.offer(updates, { model, lastMode })

  yield* Effect.forkScoped(
    Effect.forever(
      Effect.gen(function* () {
        const event = yield* Queue.take(liveQueue)
        const current = yield* Ref.get(stateRef)
        if (event.sequence <= current.model.snapshotSequence) {
          return
        }
        const next: ClientState = {
          model: projectEvent(current.model, event),
          lastMode: { kind: "live" },
        }
        yield* Ref.set(stateRef, next)
        yield* Queue.offer(updates, next)
      }),
    ),
  )

  return {
    updates,
    getState: () => Ref.get(stateRef),
  } as const
})

const snapshotToReadModel = (snapshot: Snapshot): ReadModel => ({
  snapshotSequence: snapshot.snapshotSequence,
  lists: HashMap.fromIterable(snapshot.lists.map((list) => [list.id, list] as const)),
})

const loadLocalBase = Effect.fn("client.loadLocalBase")(function* (
  engine: Engine["Service"],
  afterSequence: number,
) {
  if (afterSequence <= 0) return null
  const history = yield* engine.replayFrom(0, afterSequence)
  if (history.length === 0) return emptyReadModel()
  const last = history.at(-1)
  if (!last || last.sequence !== afterSequence) {
    return null
  }
  return projectEvents(emptyReadModel(), history)
})
