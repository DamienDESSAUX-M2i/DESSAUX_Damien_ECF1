import argparse
import logging
from pathlib import Path

from src import PartenaireLibrairiesPipeline, QuotesPipeline, set_up_logger

DIR_PATH = Path(__file__).parent.resolve()


def main() -> None:
    log_dir_path = DIR_PATH / "logs"
    if not log_dir_path.exists():
        log_dir_path.mkdir()
    log_file_path = log_dir_path / "app.log"
    logger: logging.Logger = set_up_logger(
        name="app", log_file_path=log_file_path, level=logging.INFO
    )

    parser = argparse.ArgumentParser(description="Activates Pipelines")
    parser.add_argument("--quotes", action="store_true", help="Active quotes pipeline")
    args = parser.parse_args()

    if args.quotes:
        QuotesPipeline().run()

    PartenaireLibrairiesPipeline(dir_path=DIR_PATH).run()


if __name__ == "__main__":
    main()
