# Корневой conftest: кладёт корень репозитория в sys.path, чтобы в тестах
# работал `from app.disk_copy import ...`.

import pytest

from app.throttle import set_limiter


@pytest.fixture(autouse=True)
def _reset_limiter():
    """Сбрасывает процесс-глобальный лимитер вокруг каждого теста.

    Без этого залипший синглтон с уехавшим _next_slot заставит следующий тест
    спать по-настоящему (он создан с реальными time.monotonic/time.sleep).
    """
    set_limiter(None)
    yield
    set_limiter(None)
