import logging
import sys
from ingest.config import LOG_FILE


def configure_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE)),
    ]


    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=handlers,
    )

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.debug("Logging configured (debug=%s), file=%s", debug, LOG_FILE)