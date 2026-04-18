"""
llm_reporter.py — Rapport narratif LLM (OpenAI GPT-4.1-mini).

Assemble le contexte DQ complet et génère :
  - narrative markdown supply chain
  - priority_actions (liste d'actions)
  - llm_explanation / llm_suggestion par issue

Fallback : si OPENAI_API_KEY absent, timeout, ou API indisponible → rapport JSON sans narration.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from .stat_rules import AgentIssue

logger = logging.getLogger(__name__)

OPENAI_MODEL = os.environ.get("OOTILS_DQ_LLM_MODEL", "gpt-4.1-mini")
OPENAI_TIMEOUT_SECONDS = float(os.environ.get("OOTILS_DQ_LLM_TIMEOUT_SECONDS", "20"))

_SYSTEM_PROMPT = """You are a senior supply chain expert specialized in data quality management.
Your role is to analyze data quality issues detected in supply chain data (purchase orders, forecasts, 
inventory, supplier lead times) and provide actionable recommendations.

For each batch of DQ issues, you will:
1. Write a concise executive narrative (markdown) summarizing the key risks
2. List 3-5 priority actions the supply chain team should take immediately
3. For each critical issue, provide a brief explanation and a concrete suggestion

Focus on business impact: how each data quality problem affects inventory availability, 
service levels, procurement costs, and production continuity.
Be concise, direct, and action-oriented. Avoid generic advice.
Always respond in the same language as the data context."""

_USER_PROMPT_TEMPLATE = """Analyze the following supply chain data quality report and provide recommendations.

Batch context:
- entity_type: {entity_type}
- batch_id: {batch_id}
- total_rows: {total_rows}
- issues_count: {issues_count}
- critical_count: {critical_count}

Top issues by impact score:
{issues_json}

Respond with a JSON object:
{{
  "narrative": "<markdown narrative>",
  "priority_actions": ["action1", "action2", ...],
  "issue_explanations": {{
    "<issue_id>": {{
      "explanation": "<why this matters>",
      "suggestion": "<concrete fix>"
    }}
  }}
}}"""


@dataclass
class LLMReport:
    narrative: str
    priority_actions: list[str]
    issue_explanations: dict[str, dict[str, str]] = field(default_factory=dict)
    model_used: str | None = None
    llm_available: bool = True


def _build_issues_context(issues: list[AgentIssue], max_issues: int = 10) -> str:
    """Build a JSON-serializable summary of top issues for the LLM prompt."""
    # Sort by impact_score descending
    sorted_issues = sorted(
        issues,
        key=lambda i: i.impact_score or 0.0,
        reverse=True,
    )[:max_issues]

    context = []
    for issue in sorted_issues:
        context.append({
            "issue_id": str(issue.issue_id),
            "rule_code": issue.rule_code,
            "severity": issue.severity,
            "field_name": issue.field_name,
            "raw_value": issue.raw_value,
            "message": issue.message,
            "impact_score": issue.impact_score,
            "active_shortages_count": issue.active_shortages_count,
            "affected_items": issue.affected_items[:5],
        })

    return json.dumps(context, indent=2, default=str)


def generate_llm_report(
    issues: list[AgentIssue],
    entity_type: str,
    batch_id: UUID,
    total_rows: int,
) -> LLMReport:
    """
    Call OpenAI to generate a narrative report.
    Falls back to a structured JSON report if API is unavailable.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.info("OPENAI_API_KEY not set — using fallback report")
        return _fallback_report(issues, entity_type)

    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed — using fallback report")
        return _fallback_report(issues, entity_type)

    critical_count = sum(1 for i in issues if i.severity == "error")
    issues_json = _build_issues_context(issues)

    user_message = _USER_PROMPT_TEMPLATE.format(
        entity_type=entity_type,
        batch_id=str(batch_id),
        total_rows=total_rows,
        issues_count=len(issues),
        critical_count=critical_count,
        issues_json=issues_json,
    )

    client = OpenAI(api_key=api_key, timeout=OPENAI_TIMEOUT_SECONDS)
    candidate_models = [OPENAI_MODEL, *_fallback_models()]
    last_error: Exception | None = None

    for model_name in candidate_models:
        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
                max_tokens=2000,
                timeout=OPENAI_TIMEOUT_SECONDS,
            )

            content = response.choices[0].message.content
            data = json.loads(content)

            report = LLMReport(
                narrative=data.get("narrative", ""),
                priority_actions=data.get("priority_actions", []),
                issue_explanations=data.get("issue_explanations", {}),
                model_used=model_name,
                llm_available=True,
            )

            for issue in issues:
                explanation = report.issue_explanations.get(str(issue.issue_id), {})
                if explanation:
                    issue.llm_explanation = explanation.get("explanation")
                    issue.llm_suggestion = explanation.get("suggestion")

            return report
        except Exception as exc:
            last_error = exc
            logger.warning("LLM API call failed on model %s (%s)", model_name, exc)

    logger.warning("All LLM attempts failed — using fallback report (%s)", last_error)
    return _fallback_report(issues, entity_type)


def _fallback_models() -> list[str]:
    raw = os.environ.get("OOTILS_DQ_LLM_FALLBACK_MODELS", "")
    models = [model.strip() for model in raw.split(",") if model.strip()]
    return [model for model in models if model != OPENAI_MODEL]


def _fallback_report(issues: list[AgentIssue], entity_type: str) -> LLMReport:
    """Generate a structured report without LLM when API is unavailable."""
    critical = [i for i in issues if i.severity == "error"]
    warnings = [i for i in issues if i.severity == "warning"]

    # Build rule summary
    rule_counts: dict[str, int] = {}
    for issue in issues:
        rule_counts[issue.rule_code] = rule_counts.get(issue.rule_code, 0) + 1

    rules_summary = ", ".join(
        f"{code}: {count}" for code, count in sorted(rule_counts.items())
    )

    narrative = f"""## DQ Agent Report — {entity_type}

**{len(issues)} issues détectées** ({len(critical)} erreurs, {len(warnings)} warnings)

### Résumé par règle
{rules_summary if rules_summary else "Aucune issue"}

### Issues critiques
"""
    for issue in critical[:5]:
        narrative += f"- **{issue.rule_code}**: {issue.message}\n"

    if not critical:
        narrative += "_Aucune erreur critique._\n"

    priority_actions = []
    if critical:
        priority_actions.append(
            f"Résoudre {len(critical)} erreur(s) critique(s) avant traitement du batch"
        )
    for issue in sorted(issues, key=lambda i: i.impact_score or 0, reverse=True)[:3]:
        if issue.active_shortages_count > 0:
            priority_actions.append(
                f"Vérifier {issue.rule_code} sur {issue.field_name} "
                f"(impact {issue.active_shortages_count} shortage(s) actif(s))"
            )

    return LLMReport(
        narrative=narrative,
        priority_actions=priority_actions,
        issue_explanations={},
        model_used=None,
        llm_available=False,
    )
