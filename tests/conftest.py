import contextlib
import os
import time


def set_env() -> None:
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    os.environ["TZ"] = "UTC"
    with contextlib.suppress(AttributeError):
        time.tzset()
