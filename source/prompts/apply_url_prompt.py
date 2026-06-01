from __future__ import annotations


def build_apply_url_prompt(page_text: str, page_url: str, main_domain: str) -> str:
    return f"""
You are an expert at extracting job application details from job posting pages. Analyse the page content and return ONLY valid JSON.

## YOUR TASK
Determine whether this page is an accessible job posting and, if so, extract exactly how a candidate should apply.

## CRITICAL RULES
- NEVER guess, construct, or infer URLs or emails — only report what is LITERALLY present in page_text.
- apply_url, apply_email, and apply_document_url must be verbatim from page_text or null.
- If the page is not accessible, set page_accessible=false and all apply fields to null.
- apply_document_url must be populated whenever a downloadable form, PDF, Word doc, or application pack URL appears in page_text — even if it is not the primary application method.
- apply_email must be populated whenever an email address for sending applications appears in page_text — even if it is not the primary application method.
- apply_url must be populated whenever a direct application link (Apply Now, ATS link, platform URL) appears in page_text — even if it is not the primary application method.

## PAGE ACCESSIBILITY
Classify the page access status:
- "accessible"      — Page loads, shows a real job title + description + some application method
- "bot_detected"    — Bot check, captcha, Cloudflare challenge, "access denied", "verify you are human"
- "login_required"  — Must create an account or log in to view the full job or apply
- "not_found"       — 404, "this vacancy has been closed", "job no longer available", "position filled"
- "error"           — HTTP error message, blank page, timeout message in content
- "empty"           — Page loaded but has no meaningful job content

Only set page_accessible=true for "accessible" status. All other statuses → page_accessible=false, skip apply extraction.

## MEANS OF APPLICATION — PRIMARY METHOD
Choose the single most direct, actionable method a candidate uses to SUBMIT their application:

- "url"      — A direct web link/button to apply online (e.g. "Apply Now" → https://..., or an external ATS link)
- "email"    — Candidate emails their CV/application to a specific address (e.g. "send CV to jobs@company.com")
- "document" — Candidate must download, complete, and return a form as the PRIMARY submission step (not just a supplementary download)
- "platform" — Application is via a named external platform (Workday, Greenhouse, Lever, iCIMS, etc.)
- "unknown"  — Page is accessible and job-related, but no clear submission method is visible

## DECISION RULES FOR PRIMARY METHOD
1. Page inaccessible → page_accessible=false, means_of_application="unknown", all apply fields null.
2. "Apply Now" / "Apply" link present → means_of_application="url", set apply_url verbatim.
3. Named external ATS/platform mentioned or linked → means_of_application="platform", set apply_url to that platform URL if present.
4. Candidate must download a form AND email it back → means_of_application="email" (email is the submission step); set BOTH apply_email AND apply_document_url verbatim.
5. Candidate must download and return/post a form with no email address present → means_of_application="document"; set apply_document_url verbatim.
6. Candidate emails CV directly with no form involved → means_of_application="email"; set apply_email verbatim.
7. Multiple independent submission methods → pick the most prominent/first-mentioned as primary; capture all others in additional_methods.

## ALWAYS POPULATE ALL MATCHING FIELDS
Regardless of which method is primary, always extract every available value:
- Any email address for applications → apply_email (verbatim)
- Any document/form/pack download URL → apply_document_url (verbatim)
- Any direct application or ATS URL → apply_url (verbatim)

If a secondary method exists that is not captured by the three top-level fields, add it to additional_methods.

## RESPONSE SCHEMA
{{
  "page_accessible": boolean,
  "page_access_status": "accessible" | "bot_detected" | "login_required" | "not_found" | "error" | "empty",
  "page_access_detail": string | null,
  "means_of_application": "url" | "email" | "document" | "platform" | "unknown",
  "apply_url": string | null,
  "apply_email": string | null,
  "apply_document_url": string | null,
  "additional_methods": [{{"type": "url" | "email" | "document", "value": string}}] | null,
  "confidence": "high" | "medium" | "low",
  "reasoning": string
}}

## WORKED EXAMPLES

Example A — email + downloadable form (most common missed case):
Page says: "Download our application form and email your completed form to jobs@example.com"
→ means_of_application: "email"
→ apply_email: "jobs@example.com"
→ apply_document_url: "<verbatim URL of the form>"
→ additional_methods: null

Example B — apply URL only:
Page says: "Apply Now" linking to https://ats.greenhouse.io/apply/123
→ means_of_application: "url"
→ apply_url: "https://ats.greenhouse.io/apply/123"
→ apply_email: null
→ apply_document_url: null

Example C — email only, no form:
Page says: "Send your CV to recruitment@company.com"
→ means_of_application: "email"
→ apply_email: "recruitment@company.com"
→ apply_url: null
→ apply_document_url: null

Example D — apply URL + email both present:
Page says: "Apply online at https://apply.example.com or email hr@example.com"
→ means_of_application: "url"  (online apply is more direct)
→ apply_url: "https://apply.example.com"
→ apply_email: "hr@example.com"
→ additional_methods: [{{"type": "email", "value": "hr@example.com"}}]

## Page Context:
- main_domain: {main_domain}
- page_url: {page_url}
- page_text: {page_text}

Return ONLY valid JSON starting with {{ and ending with }}.
"""