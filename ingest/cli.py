import argparse
import logging
from ingest.logging_config import configure_logging
from ingest.ingest_runner import ingest_data


def main():
    parser = argparse.ArgumentParser(description="Ingest Parquet files into DB")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    configure_logging(debug=args.debug)

    logging.info("Starting ingestion job...")
    ingest_data()
    logging.info("Ingestion finished.")


if __name__ == "__main__":
    main()