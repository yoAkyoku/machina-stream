# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. In Codex tasks, use the connected GitHub tools when available; in shell-only workflows, use the `gh` CLI.

## Prerequisites

- Run issue commands from a clone with this repository's GitHub remote configured.
- This workspace's PowerShell does not currently resolve `gh` on `PATH`; the Codex GitHub connection is available for issue operations in this task.
- Shell-only workflows require `gh` to be installed and authenticated.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`.
- **Read an issue**: `gh issue view <number> --comments`, and fetch labels when needed.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments` with appropriate filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v`; `gh` does this automatically when run inside a clone.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue labelled `wayfinder:map`; child issues are tickets, labelled `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Use GitHub sub-issues where available; otherwise add tickets to the map task list and include `Part of #<map>` in each child.

Use GitHub native issue dependencies for blocking. If unavailable, record `Blocked by: #<n>, #<n>` in the ticket body. The frontier consists of open child issues with no open blockers and no assignee.

- **Claim**: assign the ticket to the driving dev before starting work.
- **Resolve**: comment with the answer, close the issue, then append a context pointer to the map's Decisions-so-far.
