---
name: My Connector
interface: transport
# backend is a FREE STRING (never a framework enum) — the provider/library you build on.
backend: TODO
# roles is the CEILING (which roles this connector CAN carry), strict-enum. The effective role
# and trust are derived framework-side from the owner-binding — NEVER declare a singular `role:`.
roles:
  - personal
# session_held: true only if a live login is held (then implement transport/quiesce). Default false.
session_held: false
# upstream shape drives the liveness cadence: socket | poll | push | none. A persistent upstream
# (socket/poll/push) MUST implement transport/health. Default none.
upstream: none
requires:
  env:
    # The secrets/config your connector reads. Provider token(s) go here. MEDIA_ENDPOINT +
    # MEDIA_TOKEN are framework-provisioned (list them only if you handle media).
    - TODO_PROVIDER_TOKEN
# The provider base URL MUST be env-overridable so the conformance harness can steer it at a stub.
# style: declare ONLY what this connector actually carries end-to-end. A flag is a claim.
style:
  images: false
# onboarding: declare interactive onboarding steps here if the provider needs pairing/login.
provenance: human-contributed
template_version: 1
---

# My Connector

TODO — one paragraph on the backend and how to get a token. Then implement the wire tools in
`src/server.ts`. See `../CONTRACT.md` for the full contract and `../examples/` for worked connectors.
