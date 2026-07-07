# PROJECT STATUS — ootils-core

> **Document de contrôle.** Propriété de l'agent `ootils-pilote` (chef de projet).

**Dernière mise à jour : 2026-07-07 (chantier H0.5 — hygiène de gouvernance).**

---

## Ce fichier n'est plus la source de vérité

Le statut vivant du projet ne vit plus ici. Il vit à deux endroits, tenus à jour au fil de l'eau :

- **Épique [#397](https://github.com/ngoineau/ootils-core/issues/397)** — commentaires datés, cases cochées au fil des merges (vagues A/B/C).
- **[`docs/ROADMAP-AGENTS-2026-H2.md`](ROADMAP-AGENTS-2026-H2.md)** — document de passation : séquencement H0→H4, critères d'acceptation par chantier, règles de vol, registre 🎯 pilote.

Ce fichier était gelé au 2026-05-30 (il nommait encore « Pyramide » comme chantier actif, depuis livré). Le figer en pointeur évite une seconde source de vérité qui dérive.

---

## État factuel au 2026-07-07 (à revérifier en live)

- **`main` = `6eb3bef`.** 70 migrations SQL (`ls src/ootils_core/db/migrations/*.sql`), 31 ADR (`ls docs/ADR-*.md`).
- **#408 (démo E2E) livrée/fermée** (7 PASS / 3 SKIP / 0 FAIL le 06/07) ; véhicule = `scripts/demo_e2e.py` + `docs/DEMO-RUNBOOK.md`.
- **#414 « allumer la base pilote »** : 3 lanes sur 4 mergées (A alias #416/ADR-031, D fix #398 #417, C bootstrap propagation #418) ; lane B = données 🎯 attendues.
- **Livré depuis la photo de mai** : DRP fair-share (ADR-028), SSE `/v1/stream` (ADR-027), machine à preuve (ADR-030), overlay param scénarisé (ADR-025), reschedule + FPO (ADR-026), tokens par agent + scopes (migration 064, ADR-029), alias de site (ADR-031).
- **Prochain** : H0.2 (première propagation pilote, `docs/RUNBOOK-pilot-propagation.md`) puis H1 (le pari AI-native devient vrai au runtime — voir ROADMAP-AGENTS §4).
