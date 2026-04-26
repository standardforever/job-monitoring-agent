def get_job_detail_and_ats_prompt(
    page_text: str,
    site_domain: str | None,
    main_domain: str,
    page_url: str | None = None,
) -> str:
    return f"""You are a job page extraction and ATS detection agent.

Analyse the page below and return a single JSON object combining full job details AND ATS classification.
Never hallucinate — only use what is explicitly present in page_text.

## Page Context
- main_domain: {main_domain}
- site_domain: {site_domain}
- page_url: {page_url}
- page_text: {page_text}

---

## PART 1 — JOB DETAIL EXTRACTION

Rules:
- Use only information visible in the page. Return null for missing values, [] for empty arrays.
- Normalize dates to ISO format where possible, but preserve original text too.
- `job_type`: "full-time" | "part-time" | null
- `contract_type`: "permanent" | "temporary" | "contract" | "freelance" | null
- `remote_option`: "remote" | "hybrid" | "on-site" | null
- `salary.period`: "annually" | "monthly" | "weekly" | "hourly" | "daily" | null
- `salary.currency`: short code (GBP, USD, EUR) or null
- `application_method.type`: "email" | "online_form" | "external_link" | "post" | "phone" | "in_person" | null
- `additional_sections`: object where keys = section names, values = full section text
- If page is not clearly a job page → set `is_job_page: false`, explain in `confidence_reason`

### MULTI-JOB PAGE SEGMENTATION

If the page contains multiple embedded vacancies, return one item per vacancy in `jobs`.

Treat each distinct role heading / vacancy section as a separate job when the page includes patterns like:
- separate role titles/headings
- separate pay/rate blocks
- separate requirements/responsibilities per role
- separate downloadable job descriptions or application forms per role

On multi-job embedded pages:
- DO NOT create a single parent "Work With Us" / "Join our team" / careers-summary job
- DO NOT merge multiple distinct roles into one object
- DO NOT place whole vacancy blocks into `additional_sections` of another job
- Shared intro text or shared apply instructions may be copied into each relevant job if they clearly apply to all roles
- `additional_sections` should only contain extra information for that specific job, not other jobs

If there are 3 distinct vacancies on the page, `jobs` must contain 3 job objects.

---

## PART 2 — ATS DETECTION

The goal is to determine if the application is handled by a third-party ATS system, 
whether or not an apply URL is visible. Work through these layers in order:

### LAYER 1 — URL-based detection (when apply_url is present)

Extract base domain by stripping subdomains and paths:
- `careers.example.com` → `example.com`
- `ukwandsworth.speedadmin.dk` → `speedadmin.dk`

Cross-domain = APPLY_BASE ≠ BASE(main_domain) → is_ats = true, confidence = high
Same-domain  = APPLY_BASE == BASE(main_domain) → is_ats = false, confidence = high

All comparisons use main_domain only, never site_domain.
If page_url itself is a specific job detail page and BASE(page_url) differs from BASE(main_domain), classify it as external_ats with is_ats = true and ats_provider = BASE(page_url).

### LAYER 2 — Vendor name detection (with or without URL)

If ANY of these vendor names appear anywhere in page_text → is_ats = true:
Workday, Greenhouse, Lever, iCIMS, SmartRecruiters, Taleo, BambooHR, Recruitee,
Teamtailor, Jobvite, SuccessFactors, SpeedAdmin, Hireful, Networx, Pinpoint, Ashby, Personio

Set ats_provider = the vendor name found.

### LAYER 3 — Behavioural phrase detection (no URL, no vendor name)

Scan page_text for these patterns and score them:

| Signal | Weight |
|---|---|
| "you will be redirected to" / "redirected to our" | Strong |
| "complete your application on" / "apply on our" | Strong |
| "hosted application form" / "application managed by" | Strong |
| "apply via our portal" / "our recruitment system" | Strong |
| "powered by" + external name | Strong |
| "create an account to apply" / "register to apply" | Moderate |
| "sign in to apply" / "log in to apply" | Moderate |
| "our recruitment partner" / "talent platform" | Moderate |
| "you will leave this site" / "external website" | Moderate |
| Iframe or embedded widget referencing external system | Moderate |

Scoring:
- 1+ Strong signal → is_ats = true, confidence = medium
- 2+ Moderate signals → is_ats = true, confidence = medium  
- 1 Moderate signal alone → is_ats = null, confidence = uncertain
- No signals at all → proceed to Layer 4

### LAYER 4 — Non-ATS confirmation (eliminates false positives)

If ANY of these are present AND no ATS signals exist → is_ats = false, confidence = high:
- mailto: link using the org's own email domain
- "email your CV to" / "send your application to" / "post your CV to"
- "download and return the form" / "apply using the form below"
- HTML form action pointing to same base domain as main_domain
- "apply in person" / "call us to apply"

### LAYER 5 — Fallback

If no signals of any kind are found:
- Apply button/link present but zero ATS signals → is_ats = false, confidence = high
- Accessible same-domain job page with no apply method visible and zero ATS signals → is_ats = false, application_type = unknown, confidence = high
- Accessible cross-domain job detail page → is_ats = true, application_type = external_ats, confidence = high, ats_provider = BASE(page_url)
- Page is blocked, inaccessible, errored, or not actually job-related → is_ats = null, application_type = unknown, confidence = uncertain

---

## GUARDRAILS

- An apply button or "Apply Now" link ALONE is NOT evidence of ATS. Always check layers 2–4.
- A login/register wall MAY indicate ATS but is not conclusive alone — look for supporting signals.
- "Powered by [name]" in page footer counts as a vendor signal even if not near the apply button.
- If page_text contains an iframe or script tag referencing an external domain, treat as moderate ATS signal.
- Never set is_ats = true based solely on the existence of an external-looking URL without domain comparison.
- A specific job detail page hosted on a different base domain from main_domain is a cross-domain recruitment/application host and should be treated as ATS unless explicit non-ATS evidence exists.
- If signals conflict (e.g. mailto present AND vendor name present) → confidence = uncertain, note in additional_notes.
- If the page is accessible and clearly job-related, no ATS evidence means is_ats=false, not null.

---

## application_type resolution

| Type | When |
|---|---|
| `external_ats` | Cross-domain apply URL confirmed |
| `embedded_form` | ATS system embedded on same page via iframe/widget |
| `native_form` | Form/apply on same base domain, no ATS signals |
| `email_application` | mailto or explicit email/post instruction |
| `login_required` | Auth wall present before apply method revealed |
| `redirect_required` | Apply action present but destination URL not in page_text |
| `unknown` | Insufficient information |

---

## ats_provider resolution

- Vendor name found in text → that vendor's display name
- Cross-domain URL, no known vendor match → base domain of apply_url
- Text signals only, no vendor name → null
- is_ats = false → null

---

## PAGE ACCESS STATUS
- `accessible` — loaded normally
- `bot_detected` — CAPTCHA, Cloudflare, access denied
- `login_required` — auth wall present
- `not_found` — 404
- `empty_or_blank` — no meaningful content
- `error` — 500/503/maintenance

---

## RESPONSE SCHEMA

Return ONLY valid JSON. No markdown. Start with {{ end with }}.

{{
  "jobs": [
    {{
      "is_job_page": true,
      "confidence_reason": "",
      "page_access_status": "accessible" | "bot_detected" | "login_required" | "not_found" | "empty_or_blank" | "error",
      "page_access_issue_detail": null,

      "title": null,
      "company_name": null,
      "holiday": null,
      "location": {{
        "address": null, "city": null, "region": null, "postcode": null, "country": null
      }},
      "salary": {{
        "min": null, "max": null, "currency": null, "period": null,
        "actual_salary": null, "raw_text_salary": null
      }},
      "job_type": null,
      "contract_type": null,
      "remote_option": null,
      "hours": {{ "weekly": null, "daily": null, "details": null }},
      "closing_date": {{ "iso_format": null, "raw_text": null }},
      "interview_date": {{ "iso_format": null, "raw_text": null }},
      "start_date": {{ "iso_format": null, "raw_text": null }},
      "post_date": {{ "iso_format": null, "raw_text": null }},
      "contact": {{ "name": null, "email": null, "phone": null }},
      "job_reference": null,
      "description": null,
      "responsibilities": [],
      "requirements": [],
      "benefits": [],
      "company_info": null,
      "how_to_apply": null,
      "additional_sections": {{}},
      "application_method": {{
        "type": null, "url": null, "email": null, "instructions": null
      }},

      "is_ats": null,
      "is_job_related": true,
      "ats_confidence": "high" | "medium" | "low" | "uncertain",
      "application_type": "external_ats" | "embedded_form" | "native_form" | "email_application" | "login_required" | "redirect_required" | "unknown",
      "ats_provider": null,
      "apply_url": null,
      "apply_button_text": null,
      "detail_button": null,
      "requires_scraping": false,
      "indicators_found": [],
      "page_validity_issues": null,
      "additional_notes": null
    }}
  ]
}}
"""
