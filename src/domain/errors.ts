import * as Schema from "effect/Schema"

export class CommandInvariantError extends Schema.TaggedErrorClass<CommandInvariantError>()(
  "CommandInvariantError",
  {
    commandType: Schema.String,
    detail: Schema.String,
  },
) {}

export class CommandPreviouslyRejectedError extends Schema.TaggedErrorClass<CommandPreviouslyRejectedError>()(
  "CommandPreviouslyRejectedError",
  {
    commandId: Schema.String,
    detail: Schema.String,
  },
) {}

export class UnknownCommandError extends Schema.TaggedErrorClass<UnknownCommandError>()(
  "UnknownCommandError",
  {
    commandId: Schema.String,
  },
) {}
