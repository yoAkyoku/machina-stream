# Domain Docs

Before exploring, read `CONTEXT.md` at the repo root and relevant records in `docs/adr/`. If a file does not exist, proceed silently. Create these files lazily when a term or decision is resolved.

This repo uses a single context:

```text
/
├── CONTEXT.md
└── docs/adr/
```

`CONTEXT.md` is a glossary, not a spec or implementation plan. Use its canonical terms in issue titles and outputs. Surface conflicts with existing ADRs rather than silently overriding them.
