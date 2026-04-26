from __future__ import annotations


def build_ats_check_prompt(
    page_text: str,
    site_domain: str | None = None,
    main_domain: str | None = None,
    page_url: str | None = None,
) -> str:
    resolved_site_domain = site_domain or page_url
    resolved_main_domain = main_domain or site_domain or page_url
    return f"""
You are an ATS detection expert. Analyse the page and return ONLY valid JSON.

## CRITICAL RULES
- NEVER guess, construct, or infer URLs — only report what is LITERALLY in page_text.
- apply_url must be verbatim from page_text or null.
- is_ats = true REQUIRES: (a) apply_url explicitly present AND cross-domain, OR (b) explicit ATS vendor/text signals in page_text. An apply button alone is NOT sufficient.
- ALL domain comparisons use main_domain only, never site_domain.
- is_ats = true REQUIRES ats_provider to be populated.

---

## BASE DOMAIN EXTRACTION
Strip subdomains and paths. Use the registrable root only.
- `careers.example.com` → `example.com`
- `ukwandsworth.speedadmin.dk` → `speedadmin.dk`
- `uksportinstitute.current-vacancies.com` → `current-vacancies.com`

Cross-domain = APPLY_BASE ≠ BASE(main_domain) → is_ats = true, application_type = external_ats
Same-domain = APPLY_BASE == BASE(main_domain) → is_ats = false

If the page_url itself is a specific job detail page and BASE(page_url) ≠ BASE(main_domain), treat it as external_ats with is_ats=true.

---

## ATS DETECTION SIGNALS

### Strong (high confidence):
- Known vendor name in text: Workday, Greenhouse, Lever, iCIMS, SmartRecruiters, Taleo, BambooHR, Recruitee, Teamtailor, Jobvite, SuccessFactors, SpeedAdmin, Hireful, Networx, Pinpoint, Ashby, Personio
- Phrases: "redirected to", "complete your application on", "apply via our portal", "hosted application form", "application managed by", "our recruitment system"
- Embedded iframe/script pointing to external domain
- Cross-domain apply_url present in page_text

### Moderate (medium confidence):
- "create an account to apply", "sign in to apply", "register to apply"
- Reference to external "recruitment partner" or "talent platform"

### Non-ATS signals:
- mailto: link with org's own email domain
- "email your CV to", "download and return the form"
- HTML form action on same base domain

---

If apply_url base domain matches a known vendor → set ats_provider to vendor display name.
If cross-domain but unknown vendor → ats_provider = base domain of apply_url.
If ATS detected via text only → ats_provider = vendor name from text, or null if unidentifiable.

---

## PAGE VALIDITY
is_job_related = true ONLY IF: specific job title + description/responsibilities + application method visible.
is_job_related = false if: 404, expired/filled, generic listings page, no application method at all.

page_access_status: "accessible" | "bot_detected" | "login_required" | "not_found" | "empty_or_blank" | "error"
page_access_issue_detail: short description of issue, or null if accessible.

---

## APPLICATION TYPE
- external_ats — cross-domain apply URL
- embedded_form — external system embedded on page
- native_form — same base domain as main_domain
- email_application — mailto or explicit email CV instruction
- login_required — auth required before apply method shown
- redirect_required — apply action present but destination URL not in page_text
- unknown — insufficient info

---

## DECISION RULES

1. page_url is a specific job detail page AND cross-domain from main_domain → is_ats=true, external_ats, confidence=high, ats_provider=BASE(page_url)
2. apply_url found AND cross-domain → is_ats=true, external_ats, confidence=high, ats_provider REQUIRED
3. apply_url found AND same-domain → is_ats=false, native_form/redirect_required, confidence=high
4. No apply_url BUT strong text ATS signals → is_ats=true, confidence=medium, apply_url=null
5. Apply button/link present but NO ATS signals in text → is_ats=false, confidence=high, application_type by context
6. Accessible same-domain job page with no apply URL, no ATS vendor/signals, and no apply method visible → is_ats=false, confidence=high, application_type=unknown
7. Page not job-related → is_job_related=false, is_ats=null, confidence=high
8. Conflicting signals, blocked pages, inaccessible pages, or pages that are not job-related → is_ats=null, confidence=uncertain/high as appropriate

---

## SELF-CHECK BEFORE RETURNING
- is_ats=true → apply_url populated OR confidence=medium (text-only)
- is_ats=true → ats_provider MUST be set (never null)
- Cross-domain claim → apply_url must be verbatim in response
- Cross-domain page_url claim → use BASE(page_url) as ats_provider
- Apply button alone with no ATS text signals → is_ats=false
- If the page is accessible and clearly job-related, no ATS evidence means is_ats=false, not null

---

## RESPONSE SCHEMA
{{
  "is_ats": boolean | null,
  "confidence": "high" | "medium" | "low" | "uncertain",
  "is_job_related": boolean,
  "application_type": "external_ats" | "embedded_form" | "native_form" | "email_application" | "login_required" | "redirect_required" | "unknown",
  "ats_provider": string | null,
  "reasoning": string,
  "apply_url": string | null,
  "apply_button_text": string | null,
  "detail_button": string | null,
  "requires_scraping": boolean,
  "indicators_found": list[string],
  "page_validity_issues": list[string] | null,
  "additional_notes": string | null,
  "page_access_status": "accessible" | "bot_detected" | "login_required" | "not_found" | "empty_or_blank" | "error",
  "page_access_issue_detail": string | null
}}

## Page Context:
- main_domain: {resolved_main_domain}
- site_domain: {resolved_site_domain}
- page_url: {page_url}
- page_text: {page_text}

Return ONLY valid JSON starting with {{ and ending with }}.
"""
