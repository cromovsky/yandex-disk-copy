"""Проверка записи одного тестового файла на общий Диск по email сотрудника."""

import hashlib
import os
import sys
import time
import uuid
from contextlib import closing
from getpass import getpass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from app.migration.destination import (
    OnPremDiskDestination, account_uid, account_upn, destination_path, normalize_destination_url,
    personal_oauth_url,
)
from app.migration.errors import MigrationError, https_url


VERIFY_TIMEOUT = 60
POLL_INTERVAL = 2


def disk_address(value):
    """Return the canonical API origin/path and any disk label supplied by the web URL."""
    url = urlsplit(https_url(value, "invalid_probe_address"))
    label = None
    if url.hostname.startswith("disk."):
        parts = url.path.rstrip("/").split("/")
        if (url.port not in (None, 443) or "?" in value or "#" in value
                or parts[:3] != ["", "client", "vd"] or len(parts) < 4
                or parts[4:] not in ([], ["disk"])):
            raise MigrationError("invalid_probe_address")
        label = parts[3]
        destination_path({"destination_type": "shared", "destination_vd_hash": label}, "/")
        value = "https://cloud-api." + url.hostname.removeprefix("disk.")
    personal_oauth_url(value)
    return normalize_destination_url(value), label


def run_probe(api_url, email, vd_hash, client_id, client_secret, owner_token):
    shared = {"destination_url": api_url, "destination_type": "shared",
              "destination_vd_hash": vd_hash}
    destination_path(shared, "disk:/")
    if not account_upn(email):
        raise MigrationError("invalid_operator_email")
    exchange = {"destination_url": api_url, "destination_oauth_url": personal_oauth_url(api_url),
                "destination_auth": "service_application", "destination_identity": email,
                "destination_client_id": client_id}
    print("1. Получаю временный токен по email и проверяю UID сотрудника.", flush=True)
    with closing(OnPremDiskDestination(exchange, {"destination_client_secret": client_secret,
                                                 "destination_owner_token": owner_token})) as issuer:
        issuer.info()
        uid, token = issuer.expected_uid, issuer.token

    print("2. Проверяю метку общего Диска, права на чтение и запись и доступ к корню.", flush=True)
    with closing(OnPremDiskDestination(shared, {"destination_token": token})) as disk:
        info = disk.check_available()
        operator = info.get("user")
        if not uid or not isinstance(operator, dict) or uid != account_uid(operator.get("uid")):
            raise MigrationError("destination_identity_mismatch")

        probe_id = uuid.uuid4().hex
        path = f"disk:/disk-copy-probe-{probe_id}.txt"
        content = f"Yandex Disk Copy shared disk write test {probe_id}\n".encode("ascii")
        digest = hashlib.md5(content).hexdigest()
        if disk.stat(path) is not None:
            raise MigrationError("destination_conflict")

        print(f"3. Записываю один файл. Общий Диск {vd_hash}, путь {path}, UID {uid}.", flush=True)
        with TemporaryDirectory() as folder:
            local_file = Path(folder) / "probe.txt"
            local_file.write_bytes(content)
            try:
                operation = disk.upload(path, local_file, overwrite=False)
            except MigrationError as exc:
                if not exc.uncertain:
                    raise
                print(f"Ответ на загрузку не подтверждён ({exc.code}). Проверяю файл по пути.")
                operation = None

        print("4. Проверяю итоговый файл на этом общем Диске.", flush=True)
        deadline = time.monotonic() + VERIFY_TIMEOUT
        while True:
            if operation:
                status = disk.poll(operation)
                if status == "failed":
                    raise MigrationError("destination_operation_failed")
                if status == "success":
                    operation = None
            if not operation:
                entry = disk.stat(path)
                if entry is not None:
                    if (entry.get("type") != "file" or entry.get("size") != len(content)
                            or entry.get("md5") != digest):
                        raise MigrationError("destination_verification_failed")
                    print(f"Запись подтверждена. Размер {len(content)} байт, MD5 {digest}.")
                    return
            if time.monotonic() >= deadline:
                raise MigrationError("destination_verification_timeout", uncertain=True)
            time.sleep(POLL_INTERVAL)


def field(name, prompt, secret=False):
    value = os.environ.get(name)
    if not value and sys.stdin.isatty():
        value = (getpass if secret else input)(prompt + ": ")
    if not value or not value.strip():
        print(f"Требуется параметр {name}.", file=sys.stderr)
        raise MigrationError("invalid_probe_input")
    return value if secret else value.strip()


def main():
    print("Проверка оставит один небольшой .txt в корне выбранного общего Диска.")
    try:
        api, linked_disk = disk_address(field("OP_DISK_API", "Адрес Disk API или ссылка на общий Диск"))
        email = field("OP_SHARED_OPERATOR_EMAIL", "Email сотрудника с доступом на запись")
        if not account_upn(email):
            raise MigrationError("invalid_operator_email")
        if linked_disk and not os.environ.get("OP_VD_HASH"):
            label = linked_disk
        else:
            label = field("OP_VD_HASH", "Метка или ссылка на общий Диск")
            if "://" in label:
                link_api, label = disk_address(label)
                if link_api != api:
                    raise MigrationError("probe_disk_mismatch")
            else:
                label = label.rstrip("/").removesuffix("/disk")
            destination_path({"destination_type": "shared", "destination_vd_hash": label}, "/")
            if linked_disk and linked_disk != label:
                raise MigrationError("probe_disk_mismatch")
        print(f"Disk API: {api}\nМетка общего Диска: {label}", flush=True)
        run_probe(
            api, email, label,
            field("DISK_MIGRATION_CLIENT_ID", "Client ID сервисного приложения"),
            field("DISK_MIGRATION_CLIENT_SECRET", "Client secret", True),
            field("OWNER_TOKEN", "Токен владельца организации", True),
        )
    except MigrationError as exc:
        hint = {
            "invalid_probe_address": "Укажите HTTPS-адрес cloud-api.<домен>/v1/disk или ссылку на корень общего Диска вида https://disk.<домен>/client/vd/<метка>/disk.",
            "invalid_personal_disk_api_url": "Адрес Disk API установки имеет вид https://cloud-api.<домен>/v1/disk.",
            "invalid_destination_shared_disk": "Скопируйте метку сразу после /vd/ или вставьте ссылку на корень общего Диска.",
            "probe_disk_mismatch": "Адрес установки и метка в обоих полях должны указывать на один общий Диск.",
            "invalid_operator_email": "Введите полный email сотрудника с доступом к общему Диску.",
        }.get(exc.code)
        if hint:
            print(hint, file=sys.stderr)
        status = f" (HTTP {exc.http_status})" if exc.http_status else ""
        print(f"Запись не подтверждена. Код {exc.code}{status}.", file=sys.stderr)
        return 1
    except (EOFError, KeyboardInterrupt):
        print("\nПроверка прервана.", file=sys.stderr)
        return 1
    except ValueError:
        print("Некорректный адрес или параметр запроса.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
