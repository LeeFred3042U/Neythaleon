import sys
from ingest.cli import main as ingest_main
from plot.cli import main as plot_main

if __name__ == '__main__':
    # Run ingestion
    ingest_main()

    # Generate dashboard
    print('Generating dashboard...')
    plot_main()