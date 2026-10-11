"""Errors exposed to the migration journal never carry HTTP diagnostics."""

import re
from urllib.parse import urlsplit


class MigrationError(Exception):
    def __init__(self, code, *, uncertain=False, auth=False):
        safe = str(code)
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", safe):
            safe = "migration_failed"
        self.code = safe
        self.uncertain = bool(uncertain)
        self.auth = bool(auth)
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
