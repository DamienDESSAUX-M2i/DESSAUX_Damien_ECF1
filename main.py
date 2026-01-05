import logging
from pathlib import Path

from src import set_up_logger

DIR_PATH = Path(__file__).parent.resolve()


def main() -> None:
    log_dir_path = DIR_PATH / "logs"
    if not log_dir_path.exists():
        log_dir_path.mkdir()
    log_file_path = log_dir_path / "app.log"
    logger: logging.Logger = set_up_logger(
        name="app", log_file_path=log_file_path, level=logging.INFO
    )


if __name__ == "__main__":
    main()
