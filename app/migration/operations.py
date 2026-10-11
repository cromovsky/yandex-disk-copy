"""Safe operation references retain the API's path or query polling route."""

import re

from .errors import MigrationError


OPERATION_ID = re.compile(r"[A-Za-z0-9_-]{1,256}")
_REFERENCE = re.compile(r"(?:\?id=)?[A-Za-z0-9_-]{1,256}")


def valid_operation_reference(value):
    return isinstance(value, str) and _REFERENCE.fullmatch(value) is not None


def operation_endpoint(reference):
    if not valid_operation_reference(reference):
        raise MigrationError("invalid_operation_reference")
    if reference.startswith("?id="):
        return "/operations" + reference
    return "/operations/" + reference
