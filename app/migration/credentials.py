"""Ephemeral credentials bound to an immutable migration configuration."""

from __future__ import annotations

import threading

from .store import config_identity


class CredentialRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._bindings = {}
        self._credentials = {}

    def put(self, job_id, config, credentials):
        binding = config_identity(config)
        required = {"source_client_secret", "destination_token"}
        if (not isinstance(credentials, dict)
                or not required <= set(credentials)
                or set(credentials) - required - {"source_admin_token"}):
            raise ValueError("invalid_credential_fields")
        if any(
            not isinstance(value, str) or not value or len(value) > 16384
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            for value in credentials.values()
        ):
            raise ValueError("invalid_credentials")
        with self._lock:
            current = self._bindings.get(job_id)
            if current is not None and current != binding:
                raise ValueError("credential_binding_mismatch")
            self._bindings[job_id] = binding
            self._credentials[job_id] = dict(credentials)

    def get(self, job_id):
        with self._lock:
            value = self._credentials.get(job_id)
            return dict(value) if value is not None else None

    def discard(self, job_id):
        with self._lock:
            self._credentials.pop(job_id, None)

    def clear(self):
        with self._lock:
            self._credentials.clear()
