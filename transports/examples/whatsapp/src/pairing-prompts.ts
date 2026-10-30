/**
 * Pairing-method prompts for the onboarding snapshot (pure — no baileys / MCP import, so it is
 * testable as a plain tsx script; see pairing-prompts.test.ts).
 *
 * Before the user has picked a pairing method (no QR / code minted yet, status still 'waiting') the
 * connector declares the two available methods as `prompt` artifacts, so a generic frontend renders
 * its method picker from the poll instead of hardcoding "Phone Number" / "QR Code".
 *
 *   `secretKey`  — the method's key (also the frontend's tab-selection key).
 *   `label`      — OPTIONAL display name for the method.
 *   `inputType`  — OPTIONAL render hint: 'text' (default) needs a value typed first (the phone
 *                  number); 'action' pairs immediately on selection (the QR button starts the flow).
 * Both fields are optional in the contract; a frontend that ignores them still works.
 */
export interface PairingPrompt {
  kind: 'prompt'
  text: string
  secretKey: string
  label: string
  inputType: 'text' | 'action'
}

/** The state the prompts depend on: whether any pairing credential has been minted yet. */
export interface PairingProgress {
  hasImage: boolean
  hasCode: boolean
  status: string
}

export const PHONE_PROMPT: PairingPrompt = {
  kind: 'prompt',
  text: 'Enter your phone number to receive a pairing code',
  secretKey: 'phone',
  label: 'Phone Number',
  inputType: 'text',
}

export const QR_PROMPT: PairingPrompt = {
  kind: 'prompt',
  text: 'Or scan a QR code instead',
  secretKey: 'qr',
  label: 'QR Code',
  inputType: 'action',
}

/** The method prompts to show now: both while still undecided, none once a credential exists or the
 *  handshake has moved past 'waiting'. */
export function pairingMethodPrompts(p: PairingProgress): PairingPrompt[] {
  if (p.hasImage || p.hasCode || p.status !== 'waiting') return []
  return [{ ...PHONE_PROMPT }, { ...QR_PROMPT }]
}
