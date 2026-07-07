/**
 * WhatsApp connector outbound send-routing decision (S1.3 (a)+(b)).
 *
 * PURE & DEPENDENCY-FREE on purpose: no baileys / no MCP import, so it is unit-tested without the
 * folder-local node_modules (which only the conformance harness installs). `server.ts` reads the
 * live primitives (the explicit cert signal + whether the target is a deliverable WhatsApp JID) and
 * acts on this verdict.
 *
 * THE BUG THIS FIXES (live-poisoning): the old send handler echoed-and-latched on
 * `ECHO_MODE || !isDeliverableTarget(to)` — so a LIVE send to a target that merely didn't match the
 * JID regex (a malformed reply target, a newsletter/unknown JID suffix) would (1) latch
 * `conformanceSeen` → flip the live session into cert mode (disarming the live connect / poisoning
 * health/onboarding snapshots) and (2) echo the outbound straight back into the drain queue as a
 * FAKE inbound message. The cert-vs-live distinction must be an EXPLICIT signal, never inferred from
 * the target's shape.
 *
 * THE RULE:
 *   - `certRun` (ECHO_MODE || an explicit cert env || an already-latched conformanceSeen) → 'echo':
 *     loop the outbound back into the drain queue (the wire/render/media cert contract). This is the
 *     ONLY route that may echo or latch conformanceSeen.
 *   - LIVE (certRun:false) + a NON-deliverable target → 'drop_non_deliverable': the server logs it
 *     loudly and returns a not-delivered result — NEVER echoes a fake inbound, NEVER latches cert.
 *   - LIVE + a deliverable JID → 'live': the real send (the caller still verifies the socket).
 */

export type SendRoute = 'echo' | 'drop_non_deliverable' | 'live'

export interface SendRouteInput {
  /** ECHO_MODE || an explicit cert env (TRANSPORT_CONFORMANCE/WHATSAPP_ECHO) || conformanceSeen. */
  certRun: boolean
  /** `isDeliverableTarget(to)` — the target is a real WhatsApp JID we can transmit to. */
  deliverable: boolean
}

/** Pure routing verdict. See the module header for the contract. */
export function decideSendRoute({ certRun, deliverable }: SendRouteInput): SendRoute {
  // Explicit cert/echo run → loopback. The target shape is irrelevant here (the harness drives
  // synthetic targets like 'probe'); this is the ONLY route that echoes / latches conformanceSeen.
  if (certRun) return 'echo'
  // LIVE run: NEVER echo. A non-deliverable target is dropped loudly (the live-poisoning fix) —
  // inferring "cert" from a non-deliverable target is exactly the bug.
  if (!deliverable) return 'drop_non_deliverable'
  // LIVE + a deliverable JID → the real send path.
  return 'live'
}
