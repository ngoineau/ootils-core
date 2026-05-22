# Security Policy

## Supported Versions

Ootils Core is in **V1 alpha**. A reference deployment runs on the internal VM described in `docs/INFRA-RUNBOOK.md`; no public GA release exists yet.

| Version | Status | Supported |
|---------|--------|-----------|
| V1 alpha (current `main`) | Alpha / internal hardening | Security fixes accepted; expect rapid iteration |
| pre-V1 | Architecture / proof | Not supported |

The current security posture is summarised in [REVIEW-2026-05](docs/REVIEW-2026-05.md) — R3 (CORS + security headers, partially shipped) and R4 (dependency pinning + Dependabot, shipped) are the most recently closed items.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues privately:
1. Go to the [Security tab](../../security) of this repository
2. Click "Report a vulnerability"
3. Provide a clear description, steps to reproduce, and potential impact

We will acknowledge receipt within 72 hours and provide a timeline for resolution.

## Built-in Controls

- **Authentication.** Every `/v1/*` endpoint requires a Bearer token (`OOTILS_API_TOKEN`); `/health` is the only unauthenticated route. Token comparison uses `hmac.compare_digest` to avoid timing leaks. The API fails closed at startup if `OOTILS_API_TOKEN` is unset.
- **SQL.** All queries are parameterised via psycopg3 (`%s` placeholders) or `psycopg.sql.SQL` / `sql.Identifier` for dynamic identifiers. No f-string SQL.
- **Transport headers.** `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: strict-origin-when-cross-origin`, `Strict-Transport-Security` (HTTPS only), and a restrictive `Content-Security-Policy` (with a `/docs` carve-out for Swagger UI) are emitted on every response unless `OOTILS_DISABLE_SECURITY_HEADERS=1`.
- **CORS.** Disabled by default. Configure with `OOTILS_CORS_ALLOWED_ORIGINS=https://example.com,https://app.example.com`. Wildcard is intentionally not the default.
- **Container.** The Docker runtime image runs as a non-root user (`ootils`); no secrets in image layers.
- **Dependencies.** Pinned to known-good `major.minor` ranges (`~=` PEP 440). Dependabot proposes weekly updates for `pip`, `github-actions`, and `docker` ecosystems.
- **Rate limiting.** Disabled by default. Enable with `OOTILS_RATE_LIMIT_PER_MIN=60` (or any [slowapi limit string](https://github.com/laurents/slowapi)). Per-IP, headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`) emitted on every response; 429 on burst.

## Known Gaps (alpha)

- **Multi-tenancy.** Single-tenant only. RLS deferred to V2 (`docs/REVIEW-2026-05.md`, issue #195).
- **Audit log endpoints.** API request audit is written to `api_request_log` but not yet exposed.
- **Distributed rate-limit backend.** The slowapi limiter uses in-memory storage; behind a multi-replica deployment it counts per replica. A shared Redis backend is on the Phase 3 list.

## Security Considerations for a Planning Engine

Ootils handles supply chain data that may include:
- Customer order information
- Supplier contracts and pricing
- Production capacity and schedules
- Inventory positions

Deployments should sit behind a reverse proxy / WAF, apply IP allow-listing for non-public access, and rotate `OOTILS_API_TOKEN` regularly. See `docs/INFRA-RUNBOOK.md` and `docs/SECURITY-vm-hardening.md` for the reference deployment hardening checklist.
