# Contributing to Ootils

> This project now has real code, migrations, APIs, and tests.
> Contributions still shape the architecture, but they must match running repo reality, not an old white-paper snapshot.

---

## Who We're Looking For

### Supply Chain Practitioners
You've used SAP, Kinaxis, Blue Yonder, or built your own tools. You've hit the walls. You know what "the system can't explain why" feels like at 11pm before a board meeting.

Your job: challenge the business logic, the node model, the edge semantics. Tell us where our model breaks against reality.

### Graph & Engine Engineers
You understand DAGs, topological sort, incremental computation, event-driven systems. You've built things that propagate state efficiently.

Your job: challenge the propagation model. Find the edge cases. Propose better algorithms.

### Operations Research / Optimization
You know LP, MILP, constraint programming. You've modeled supply-demand problems formally.

Your job: validate that our deterministic core computation is sound. Also call out where determinism claims should be narrowed to exclude UUID generation, audit timestamps, or other non-computational metadata.

### AI / Agent Builders
You're building autonomous agents. You know what a well-designed API looks like from an agent's perspective. You've seen tools that are impossible to use programmatically.

Your job: define what "AI-native" really means for a planning engine. What does an agent need that humans don't?

---

## How to Contribute Right Now (No Code Required)

### 1. Challenge the Architecture
Read the [README](README.md) and the `/docs/` folder. Open a Discussion with:
- "This won't work because..." → we want this
- "You're missing..." → we want this
- "In my experience at [company], the real problem is..." → we *really* want this

### 2. Share Real War Stories
The business model, the node types, the edge semantics — they were designed from 20 years of real SC operations. But every supply chain is different.

Open a Discussion tagged `war-story`. Tell us about a real planning failure. How would Ootils have handled it? How should it?

### 3. Review Architecture Decision Records (ADRs)
Every significant architectural choice will be documented in `/docs/adr/`. Comment, challenge, propose alternatives.

### 4. Help Define V1 Test Cases
What are the 10 most important scenarios a supply chain planning engine must handle correctly? Open a Discussion tagged `test-case`.

---

## Contribution Principles

**No supply chain debt**
We are not recreating MRP with a better UI. Every design decision must be validated against the AI-native vision.

**Explicit over magic**
Every calculation must be traceable. If you can't explain why the engine produced a result, the design is wrong.

**API first, UI never (for now)**
We build the engine. The interface is someone else's problem for now. Do not propose UI features in V1.

**Determinism is non-negotiable**
The same inputs must always produce the same outputs. No randomness in the core engine.

**Fail loudly**
If the engine can't compute something, it should say so clearly — not silently produce a wrong answer.

---

## Code of Conduct

Be direct. Be technical. Be respectful.

We have zero tolerance for:
- Marketing speak in technical discussions
- Feature requests that contradict the AI-native vision
- "Why don't you just use [existing tool]?" without a genuine architectural argument

---

## Getting Started

1. Read [README.md](README.md) — all of it
2. Read [VISION.md](VISION.md)
3. Browse [GitHub Discussions](https://github.com/ngoineau/ootils-core/discussions)
4. Pick a thread that interests you and contribute

That's it. No CLA. No bureaucracy. Just good engineering discussions.

---

## Code Contribution Workflow

When you do want to ship code, the workflow is short.

### Set up

```bash
git clone https://github.com/ngoineau/ootils-core.git
cd ootils-core
make install            # pip install -e ".[dev]"
make pre-commit-install # ruff + hygiene hooks at commit time
```

Or follow [`docs/QUICKSTART.md`](docs/QUICKSTART.md) for the runtime side.

### Code → test → lint

```bash
make test       # unit + feature tests; no DB needed
make lint       # ruff check src/ (CI scope)
make lint-fix   # ruff --fix on src/ AND tests/
```

If you touch the engine kernel or migrations, also run the integration suite against a throwaway PostgreSQL:

```bash
# Throwaway DB on your tunnel or local docker
export DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:5432/ootils_test
export OOTILS_API_TOKEN=dev-token
make test-integration
```

> **Never** point `DATABASE_URL` at a DB you care about. The shared `migrated_db` fixture in `tests/integration/conftest.py` drops every table in the public schema at module teardown.

### Coverage gate (recommended, optional)

```bash
make coverage           # fails if overall coverage drops under 80%
COV_FAIL_UNDER=85 make coverage   # raise the bar locally before pushing
```

### Type check (non-blocking)

```bash
python -m mypy src/ootils_core
```

CI runs the same command in a `continue-on-error: true` job — the goal is visibility, not enforcement. See [REVIEW-2026-05 R6](docs/REVIEW-2026-05.md) for the staged plan.

### Conventions enforced by code review

- **No `TODO`, `FIXME`, `HACK` comments.** The repo currently has zero, and ruff in CI will flag new ones (see `pyproject.toml`).
- **Parameterised SQL only.** `cur.execute("... %s ...", (value,))` — never an f-string in a query. Use `psycopg.sql.SQL` / `sql.Identifier` for dynamic identifiers.
- **No JSONB for business data.** Diagnostic / staging payloads are the explicit carve-out (`dq_agent_runs.summary`, `mrp_runs.errors`/`warnings`, `demo_runs.artifact`); everything else uses typed columns. See [CLAUDE.md](CLAUDE.md).
- **Integration tests use the real DB, not mocks.** Mock `psycopg.Connection` and the test will be deleted.
- **Migrations are idempotent and sequential.** `IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, numbered after `031_`. Wrap in `BEGIN/COMMIT` unless you need DDL outside a transaction.
- **Every new `/v1/*` endpoint must include `_token: str = Depends(require_auth)`.** Otherwise the audit log middleware silently skips it — see `api/app.py:_should_audit_request`.

### Pull requests

- One scope per PR. Don't bundle a behavioural change with a refactor.
- Title in conventional-commit style (`feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`).
- Body explains the *why*. The *what* lives in the diff and the commit messages.
- CI must be green (lint + pytest + integration). The mypy job is non-blocking but new errors should be addressed when reasonable.

---

*The best contribution you can make right now is to tell us where we're wrong.*
