/**
 * Client catch-up — snapshot + replay + live, sequence-deduped.
 *
 * Mirrors T3 subscribeShell / subscribeThread behavior:
 * 1. Optionally load a snapshot (snapshotSequence = N)
 * 2. Attach live stream first so events during load aren't lost
 * 3. Replay events after N (or take a fresh snapshot if gap is huge)
 * 4. Apply live events; ignore sequence <= local cursor
 */

import * as Effect from "effect/Effect"
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
 * Returns a Queue of ClientState snapshots for observation (demo/tests).
 */
export const subscribeWithCatchUp = Effect.fn("client.subscribeWithCatchUp")(function* (args: {
  readonly engine: Engine["Service"]
  /** Known cursor from a previous session; omit for cold start. */
  readonly afterSequence?: number
}) {
  const { engine } = args
  const afterSequence = args.afterSequence ?? 0

  // 1. Attach live first (T3 attaches PubSub before snapshot/replay).
  const liveQueue = yield* Queue.unbounded<DomainEvent>()
  yield* Stream.runForEach(engine.streamEvents(), (event) => Queue.offer(liveQueue, event)).pipe(
    Effect.forkScoped,
  )

  // 2. Decide snapshot vs bounded replay.
  const snapshot = yield* engine.getSnapshot()
  const gap = snapshot.snapshotSequence - afterSequence

  let model: ReadModel
  let lastMode: CatchUpMode

  if (afterSequence <= 0 || gap < 0 || gap > REPLAY_LIMIT) {
    // Cold start or too far behind → take snapshot.
    model = snapshotToReadModel(snapshot)
    lastMode = { kind: "snapshot" }
  } else {
    // Warm resume → replay events after cursor onto empty + optional base.
    // For a real client you'd seed from a cached local snapshot at afterSequence.
    // Here we start from the server snapshot at afterSequence by replaying only the gap
    // onto a rebuild: simplest correct approach is rebuild from replay when we have
    // no local base. Demo path: rebuild empty + all events up through snapshot
    // is expensive; instead we take snapshot and only use replay when we have base.
    //
    // Practical warm path matching T3:
    //   localBase @ afterSequence + replay(afterSequence) → current
    // Without a local base we fall back to server snapshot.
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

  // 3. Apply live events (dedupe by sequence).
  yield* Effect.forkScoped(
    Effect.forever(
      Effect.gen(function* () {
        const event = yield* Queue.take(liveQueue)
        const current = yield* Ref.get(stateRef)
        if (event.sequence <= current.model.snapshotSequence) {
          return // overlap during catch-up
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
  lists: new Map(snapshot.lists.map((list) => [list.id, list])),
})

/**
 * Simulate a local cached base at `afterSequence`.
 * If afterSequence is current, return that snapshot as base; else null
 * (forces full snapshot — we don't persist client caches in this sample).
 */
const loadLocalBase = Effect.fn("client.loadLocalBase")(function* (
  engine: Engine["Service"],
  afterSequence: number,
) {
  // Demo-only: rebuild pure from events 1..afterSequence to act as "local cache".
  if (afterSequence <= 0) return null
  const history = yield* engine.replayFrom(0, afterSequence)
  if (history.length === 0) return emptyReadModel()
  const last = history.at(-1)
  if (!last || last.sequence !== afterSequence) {
    // partial / missing — treat as no usable cache
    return null
  }
  return projectEvents(emptyReadModel(), history)
})
