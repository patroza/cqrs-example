/**
 * Branded id schemas shared by commands, events, and read models.
 */

import * as Schema from "effect/Schema"

export const ListId = Schema.String.pipe(Schema.brand("ListId"))
export type ListId = typeof ListId.Type

export const TodoId = Schema.String.pipe(Schema.brand("TodoId"))
export type TodoId = typeof TodoId.Type

export const CommandId = Schema.String.pipe(Schema.brand("CommandId"))
export type CommandId = typeof CommandId.Type

export const EventId = Schema.String.pipe(Schema.brand("EventId"))
export type EventId = typeof EventId.Type
