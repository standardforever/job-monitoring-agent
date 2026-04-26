from __future__ import annotations

import csv
from io import BytesIO, StringIO
from pathlib import Path

from openpyxl import load_workbook

from utils.logging import get_logger, log_event

logger = get_logger("file_input_service")


class FileInputService:
    def extract_domains(self, filename: str, content: bytes) -> list[str]:
        suffix = Path(filename).suffix.lower()
        log_event(
            logger,
            "info",
            "file_domain_extraction_started filename=%s suffix=%s",
            filename,
            suffix,
            domain=filename,
            filename=filename,
            suffix=suffix,
        )

        if suffix == ".csv":
            domains = self._extract_domains_from_csv(content)
        elif suffix == ".xlsx":
            domains = self._extract_domains_from_xlsx(content)
        else:
            raise ValueError("Only .csv and .xlsx files are supported")

        if not domains:
            raise ValueError("No valid values found in the 'domain' column")

        log_event(
            logger,
            "info",
            "file_domain_extraction_completed filename=%s domain_count=%s",
            filename,
            len(domains),
            domain=domains[0],
            filename=filename,
            domain_count=len(domains),
        )
        return domains

    def _extract_domains_from_csv(self, content: bytes) -> list[str]:
        text_stream = StringIO(content.decode("utf-8-sig"))
        reader = csv.DictReader(text_stream)
        return self._collect_domain_column(reader)

    def _extract_domains_from_xlsx(self, content: bytes) -> list[str]:
        workbook = load_workbook(BytesIO(content), read_only=True, data_only=True)
        try:
            sheet = workbook.active
            rows = list(sheet.iter_rows(values_only=True))
        finally:
            workbook.close()

        if not rows:
            return []

        headers = [str(value).strip() if value is not None else "" for value in rows[0]]
        normalized_headers = [header.lower() for header in headers]
        if "domain" not in normalized_headers:
            raise ValueError("Uploaded file must contain a 'domain' column")

        domain_index = normalized_headers.index("domain")
        domains: list[str] = []
        seen: set[str] = set()
        for row in rows[1:]:
            if row is None or domain_index >= len(row):
                continue
            value = str(row[domain_index] or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            domains.append(value)
        return domains

    def _collect_domain_column(self, reader: csv.DictReader) -> list[str]:
        if reader.fieldnames is None:
            return []

        normalized_fieldnames = {str(name).strip().lower(): name for name in reader.fieldnames if name}
        if "domain" not in normalized_fieldnames:
            raise ValueError("Uploaded file must contain a 'domain' column")

        domain_key = normalized_fieldnames["domain"]
        domains: list[str] = []
        seen: set[str] = set()
        for row in reader:
            value = str((row or {}).get(domain_key) or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            domains.append(value)
        return domains
