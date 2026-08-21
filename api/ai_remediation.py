import json
import os
import re

ANTHROPIC_MODEL = "claude-sonnet-4-6"

# Phase 4 (domain-agnostic): the deterministic fallback below is
# Customer-specific by nature (email/state/country pattern heuristics only
# make sense for that field set) -- it only runs for domain_key == 'customer'.
# Every other domain gets a generic "no heuristic, needs manual review"
# response instead of a silently-wrong guess. The AI path (_ai_suggest) has
# no such limit: it works from whatever attribute keys/values the record and
# reject_reasons actually contain, for any domain, using the domain's own
# display_name/entity_label in the prompt (see suggest_remediation's
# domain_key/entity_label params) rather than a hardcoded "customer" string.
HEURISTIC_FALLBACK_DOMAINS = {"customer"}


def _client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception:
        return None


def suggest_remediation(record: dict, reject_reasons: list[str], domain_key: str, entity_label: str = None) -> dict:
    """
    Returns: {
        "suggested_fields": {...},   # proposed corrected values, only for fields it has an opinion on
        "rationale": "...",
        "source": "ai" | "heuristic_fallback"
    }
    A data steward always reviews and approves before anything is written back
    to the pipeline -- this function only proposes, it never auto-applies.

    `record` is a plain dict keyed by attribute_key (unpacked from the
    exception's `attributes` JSON), plus source_system/source_record_id.
    `entity_label` is the domain's singular display label (e.g. "Customer",
    "Product") from governance.domain_registry, used only to phrase the AI
    prompt naturally -- never hardcoded.
    """
    client = _client()
    if client is not None:
        try:
            return _ai_suggest(client, record, reject_reasons, entity_label or domain_key.title())
        except Exception as e:
            fallback = _heuristic_suggest(record, reject_reasons, domain_key)
            fallback["rationale"] = f"(AI call failed: {e}) " + fallback["rationale"]
            return fallback
    return _heuristic_suggest(record, reject_reasons, domain_key)


def _ai_suggest(client, record, reject_reasons, entity_label) -> dict:
    prompt = f"""You are assisting a data steward remediating a rejected {entity_label.lower()} master-data record.

Record (source system: {record.get('source_system')}, source id: {record.get('source_record_id')}):
{json.dumps(record, default=str, indent=2)}

This record failed these validation rules:
{json.dumps(reject_reasons, indent=2)}

Propose corrections ONLY for the fields that failed validation. Do not guess values you
have no evidential basis for (e.g. do not invent a phone number from nothing) -- in that
case, leave the field out of suggested_fields and explain in the rationale that it needs
manual steward input.

Respond with ONLY a JSON object, no other text, in this exact shape:
{{"suggested_fields": {{"<field>": "<value>", ...}}, "rationale": "<1-3 sentences>"}}"""

    resp = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    text = re.sub(r"^```(json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    parsed = json.loads(text)
    return {
        "suggested_fields": parsed.get("suggested_fields", {}),
        "rationale": parsed.get("rationale", ""),
        "source": "ai",
    }


def _heuristic_suggest(record, reject_reasons, domain_key) -> dict:
    """Deterministic rule-based fallback so the demo is fully functional
    without an API key configured. Handles the specific patterns seeded in
    this dataset's Customer domain -- see module docstring for why this
    doesn't attempt to generalize to other domains. Inspects the record's
    own attribute values directly (not reject_reasons' text), so it stays
    correct even if a rule's governed description text changes."""
    if domain_key not in HEURISTIC_FALLBACK_DOMAINS:
        return {
            "suggested_fields": {},
            "rationale": "No deterministic remediation heuristic is available for this domain yet "
                         "(only the AI path generalizes automatically); manual steward review needed.",
            "source": "heuristic_fallback",
        }

    suggestions = {}
    notes = []

    email = (record.get("email") or "")
    if email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        fixed = email.replace("[at]", "@").replace("@@", "@")
        fixed = re.sub(r"@([^.\s]+)$", r"@\1.com", fixed) if "@" in fixed and "." not in fixed.split("@")[-1] else fixed
        if fixed != email and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", fixed):
            suggestions["email"] = fixed
            notes.append(f"Corrected likely email typo: '{email}' -> '{fixed}'.")
        else:
            notes.append("Email format is invalid and no confident auto-fix was found; needs manual steward input.")

    if not (record.get("last_name") or "").strip():
        notes.append("Last name is missing. Consider checking the source system's original record or contacting the customer; no reliable auto-fix available.")

    if not (record.get("phone") or "").strip():
        notes.append("Phone number is missing. No auto-fix possible -- requires manual entry or a follow-up data request.")

    state_code = (record.get("state_code") or "").strip()
    if state_code and len(state_code) != 2:
        city = (record.get("city") or "").strip().lower()
        city_to_state = {
            "alexandria": "VA", "washington": "DC", "new york": "NY", "boston": "MA",
        }
        guess = city_to_state.get(city)
        if guess:
            suggestions["state_code"] = guess
            notes.append(f"Inferred state '{guess}' from city '{record.get('city')}'; steward should confirm.")
        else:
            notes.append("State code is invalid and could not be confidently inferred from other fields.")

    country = (record.get("country_code") or "").strip()
    if country and country.upper() != "US" and country.upper().startswith("US"):
        # In this dataset the only seeded case is a malformed 'USA1' -- an obvious near-match to 'US'
        suggestions["country_code"] = "US"
        notes.append(f"Country code '{country}' looks like a malformed 'US'; suggesting the ISO code 'US'.")
    elif country and len(country) != 2:
        notes.append("Country code is invalid and no confident auto-fix was found.")

    if not notes:
        notes.append("No specific heuristic matched this failure pattern; manual steward review needed.")

    return {
        "suggested_fields": suggestions,
        "rationale": " ".join(notes),
        "source": "heuristic_fallback",
    }
