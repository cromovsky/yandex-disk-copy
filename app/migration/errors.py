"""Journal errors carry safe codes and status numbers, never raw HTTP data."""

import math
import re
from urllib.parse import urlsplit

from .diagnostics import diagnostic_fields


OPERATION_REFERENCE_ISSUES = frozenset({
    "metadata", "url", "origin", "route", "query", "id_mismatch",
    "missing_id", "id_format",
})


class MigrationError(Exception):
    def __init__(self, code, *, uncertain=False, auth=False, http_status=None,
                 source_path=None, retry_after=None, operation_reference_issue=None,
                 diagnostics=None):
        safe = str(code)
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", safe):
            safe = "migration_failed"
        self.code = safe
        self.diagnostics = diagnostic_fields(diagnostics)
        self.operation_reference_issue = operation_reference_issue if (
            safe == "invalid_operation_reference"
            and isinstance(operation_reference_issue, str)
            and operation_reference_issue in OPERATION_REFERENCE_ISSUES
        ) else None
        self.uncertain = bool(uncertain)
        self.auth = bool(auth)
        self.http_status = http_status if (
            isinstance(http_status, int) and not isinstance(http_status, bool)
            and 100 <= http_status <= 599
        ) else None
        self.source_path = source_path if (
            isinstance(source_path, str) and source_path.startswith("disk:/")
            and len(source_path) <= 4096 and ":" not in source_path[5:]
            and "\\" not in source_path
            and not any(ord(c) < 32 or ord(c) == 127 for c in source_path)
            and not any(p in {".", ".."} for p in source_path[5:].split("/"))
        ) else None
        self.retry_after = None
        if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
            try:
                delay = float(retry_after)
            except OverflowError:
                delay = float("inf")
            if math.isfinite(delay) and delay >= 0:
                self.retry_after = delay
        super().__init__(safe)


def https_url(value, code="invalid_download_url"):
    """Validate capability URLs without exposing their contents on failure."""
    if not isinstance(value, str) or any(ord(c) <= 32 for c in value):
        raise MigrationError(code)
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme == "https" and parsed.hostname
            and parsed.username is None and parsed.password is None
            and not parsed.fragment and "\\" not in value
            and not parsed.netloc.endswith(":")
        )
        parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise MigrationError(code)
    return value


def disk_path(value):
    if not isinstance(value, str) or "\\" in value:
        raise MigrationError("invalid_disk_path")
    value = value[5:] if value.startswith("disk:") else value
    if not value.startswith("/"):
        value = "/" + value
    if any(ord(c) < 32 for c in value) or any(
        part in (".", "..") for part in value.split("/")
    ):
        raise MigrationError("invalid_disk_path")
    return "disk:" + (value.rstrip("/") or "/")
