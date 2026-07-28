/**
 * Poll an Effect predicate until true using Schedule (testable with TestClock).
 */

import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"

export class WaitTimeoutError extends Schema.TaggedErrorClass<WaitTimeoutError>()(
  "WaitTimeoutError",
  {
    detail: Schema.String,
  },
) {}

/**
 * Retry `check` until it succeeds with `true`, or fail after `attempts`.
 */
export const waitUntil = Effect.fn("waitUntil")(function* <R>(
  check: Effect.Effect<boolean, never, R>,
  options?: {
    readonly attempts?: number
    readonly interval?: Duration.Duration
    readonly detail?: string
  },
) {
  const attempts = options?.attempts ?? 200
  const interval = options?.interval ?? Duration.millis(5)
  const detail = options?.detail ?? "condition not met in time"

  // spaced first so each retry waits; andThen recurs limits total tries
  const schedule = Schedule.spaced(interval).pipe(Schedule.andThen(Schedule.recurs(attempts)))

  return yield* check.pipe(
    Effect.flatMap((ok) =>
      ok ? Effect.void : Effect.fail(new WaitTimeoutError({ detail })),
    ),
    Effect.retry(schedule),
  )
})
