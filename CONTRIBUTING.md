# Contributing

Connectors and docs are welcome. Two rules keep this repo safe and useful.

## 1. Enable the pre-commit hook

After cloning, arm the blocking hook — it rejects any commit containing a secret, token, or
deployment/network identifier:

```
git config core.hooksPath scripts
```

The patterns live in [`.guardrails`](.guardrails). If a pattern ever false-positives on legitimate
content, bypass a single commit with `git commit --no-verify` and open an issue so we can tighten it.

## 2. Sanitize before you PR a connector

A connector authored on your own machine may embed things specific to you — provider tokens, phone
numbers, a personal email, absolute home paths, a tunnel hostname. Before opening a PR:

- Remove all credentials. Secrets belong in the connector's `requires.env`, never in source.
- Strip machine-specific paths and hostnames.
- Confirm it **certifies clean** in a fresh directory:

  ```
  npx my-agent-transport-kit certify ./your-connector
  ```

Add it under `transports/examples/<backend>/`. A certified, sanitized connector for a backend that
isn't here yet is the most valuable contribution — it becomes the worked example every other builder
(human or agent) starts from.

## Where things live

- [`transports/CONTRACT.md`](transports/CONTRACT.md) — the connector contract (read first).
- [`transports/references/`](transports/references) — pairing pitfalls, push-gateway baseline.
- [`packages/transport-kit`](packages/transport-kit) — the conformance harness + `certify` CLI.
