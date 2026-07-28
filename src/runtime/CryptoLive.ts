/**
 * Node-backed Crypto layer for Effect's Crypto service.
 */

import { createHash, randomBytes } from "node:crypto"

import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

const nodeDigestName = (algorithm: Crypto.DigestAlgorithm): string => {
  switch (algorithm) {
    case "SHA-1":
      return "sha1"
    case "SHA-256":
      return "sha256"
    case "SHA-384":
      return "sha384"
    case "SHA-512":
      return "sha512"
  }
}

export const CryptoLive = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(randomBytes(size)),
    digest: (algorithm, data) =>
      Effect.succeed(new Uint8Array(createHash(nodeDigestName(algorithm)).update(data).digest())),
  }),
)
