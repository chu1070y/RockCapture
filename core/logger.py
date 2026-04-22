import logging
import threading
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("logs")

_FMT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_SETUP_LOCK = threading.Lock()
_IS_CONFIGURED = False


def setup_logging(level: int = logging.INFO) -> None:
    """
    Initialize application logging.

    This function is intentionally idempotent because the pipeline object is
    created per API request. Without the guard, each request would append new
    handlers to the root logger and every later log line would be duplicated.
    """
    global _IS_CONFIGURED

    with _SETUP_LOCK:
        root = logging.getLogger()
        root.setLevel(level)

        if _IS_CONFIGURED:
            for noisy in ("py4j", "pyspark"):
                logging.getLogger(noisy).setLevel(logging.WARNING)
            return

        LOG_DIR.mkdir(exist_ok=True)

        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = LOG_DIR / f"lakehouse_{run_ts}.log"

        formatter = logging.Formatter(_FMT, datefmt=_DATE_FMT)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        root.addHandler(console_handler)
        root.addHandler(file_handler)

        for noisy in ("py4j", "pyspark"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

        _IS_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
