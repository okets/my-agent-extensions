/**
 * Focused logic test for the pairing-method prompts (label + inputType). Like health.test.ts, run as
 * a pure assertion script with a tsx binary:
 *   ../../../packages/core/node_modules/.bin/tsx src/pairing-prompts.test.ts   (or: npx tsx src/pairing-prompts.test.ts)
 * Exits non-zero on the first failed assertion.
 */
import { pairingMethodPrompts } from './pairing-prompts.js'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

const fresh = pairingMethodPrompts({ hasImage: false, hasCode: false, status: 'waiting' })
eq('undecided: exactly two prompts', fresh.length, 2)
eq('phone prompt carries label + inputType text', fresh[0], {
  kind: 'prompt',
  text: 'Enter your phone number to receive a pairing code',
  secretKey: 'phone',
  label: 'Phone Number',
  inputType: 'text',
})
eq('QR prompt carries label + inputType action', fresh[1], {
  kind: 'prompt',
  text: 'Or scan a QR code instead',
  secretKey: 'qr',
  label: 'QR Code',
  inputType: 'action',
})
eq('every prompt has a label and a valid inputType', fresh.every(p => typeof p.label === 'string' && (p.inputType === 'text' || p.inputType === 'action')), true)

eq('QR minted → no prompts', pairingMethodPrompts({ hasImage: true, hasCode: false, status: 'waiting' }), [])
eq('code minted → no prompts', pairingMethodPrompts({ hasImage: false, hasCode: true, status: 'waiting' }), [])
for (const s of ['linked', 'expired', 'error', 'preflight_ok', 'preflight_rejected']) {
  eq(`status ${s} → no prompts`, pairingMethodPrompts({ hasImage: false, hasCode: false, status: s }), [])
}

// Returned prompts are copies: mutating one must not leak into the next snapshot.
fresh[0].label = 'mutated'
eq('snapshots are independent copies', pairingMethodPrompts({ hasImage: false, hasCode: false, status: 'waiting' })[0].label, 'Phone Number')

if (failures) { console.error(`\n${failures} failure(s)`); process.exit(1) }
console.log('\nall pairing-prompts assertions passed')
