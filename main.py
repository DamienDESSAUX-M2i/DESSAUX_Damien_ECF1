import logging
from pathlib import Path

from src.utils.logger import set_up_logger

DIR_PATH = Path(__file__).resolve()


def main() -> None:
    log_dir = DIR_PATH / "logs"
    if not log_dir.exists():
        log_dir.mkdir()
    logger_path = log_dir / "app.log"
    logger: logging.Logger = set_up_logger(
        name="app", path=logger_path, level=logging.INFO
    )


if __name__ == "__main__":
    main()
