# Capability examples — inspiration, not canon

This shelf holds **worked capability examples**: real, self-contained capability folders that agents
(and their humans) have built and run, contributed back so the next builder starts from a proven
shape instead of a blank page.

> **Read these for patterns. Build your own.**
>
> Unlike [`transports/`](../transports), the capability shelf is **not canonical**. There is no
> certification here, no contract to conform to, and no parity obligation. An example shows *one*
> way a capability was wired for *one* agent's providers — not the way. Take the shape (how it
> degrades honestly, how it falls back between providers, how its script talks to the framework) and
> **re-derive it for your own user and providers.** Never install an example verbatim.

Because nothing on this shelf claims to be canonical, there is deliberately **no fetch-and-install
machinery** for it — the framework reads the shelf to *inspire* a build, and the agent authors a
fresh, user-shaped capability through its normal capability-build loop. (Connectors work the opposite
way: `transports/` IS canonical, is certified, and is provisioned into place.)

## Layout

```
capabilities/
├── README.md            # this file
└── examples/
    └── <name>/          # a worked capability: CAPABILITY.md + scripts/ + config.yaml + DECISIONS.md
```

Each example is a normal capability folder:

- **`CAPABILITY.md`** — YAML frontmatter (`name`, `provides`, `interface`, `requires.env`) + how the
  brain uses it. This is the contract and the I/O.
- **`scripts/`** — the actual implementation (a stateless shell script for `interface: script`, or an
  MCP server for `interface: mcp`) plus a `smoke.sh` health check.
- **`config.yaml`** — non-secret, tunable settings. Secrets are declared in `requires.env` and live in
  the agent's `.env`, never here.
- **`DECISIONS.md`** — *why* it is shaped this way. This is the part that carries the real learning;
  read it first.

## How the framework uses this shelf

The `capability-brainstorming` skill consults the shelf **before designing** a new capability: it
lists the examples, reads the closest one for its shape, and adapts that pattern to the user's chosen
provider. If the shelf can't be reached, the build simply proceeds from scratch — the shelf is
inspiration, never a dependency.

## Contributing an example back

Built a capability whose integration taught you something — a non-obvious fallback, a keyless default,
a clean honest-degrade? Contribute it so others can adapt it. An example **earns a spot when its
integration carries real learning** — this is a curated shelf of instructive shapes, not a dumping
ground for every capability ever built.

Open a PR adding `capabilities/examples/<name>/` after **sanitizing everything specific to your
machine**: real names, phone numbers, emails, API keys, private filesystem paths, host/network
identifiers, and any runtime artifacts (activation markers, run-directory job ids, per-install
paths). Keep the `DECISIONS.md` — rewritten to teach the shape, not to log one install.

## License

MIT — see [LICENSE](../LICENSE).
