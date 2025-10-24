# main.py
import logging
import sys
from pathlib import Path

from ui import run_app
from db import init_db


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def main():
    setup_logging()
    logger = logging.getLogger("main")

    base_dir = Path(__file__).parent
    db_path = base_dir / "rules.db"

    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1]).resolve()
        logger.info(f"Using custom DB path: {db_path}")

    init_db(db_path, schema_path=None)
    run_app(db_path)


if __name__ == "__main__":
    main()
