import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("logs")

_FMT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """
    애플리케이션 전체 로깅 초기화.
    main.py 진입점에서 1회 호출.

    - 콘솔: INFO 이상 출력
    - 파일:  DEBUG 이상, logs/lakehouse_YYYYMMDD_HHMMSS.log (실행마다 신규 파일)
    """
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

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # pyspark / py4j 내부 로그 억제
    for noisy in ("py4j", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """모듈별 logger 반환. 각 파일에서 get_logger(__name__) 으로 사용."""
    return logging.getLogger(name)
