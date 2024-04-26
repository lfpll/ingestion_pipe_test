import argparse
import logging
import os

from ingestion.runner import Runner
from ingestion.source import IngestCSV
from ingestion.target import PostgresTarget

logging.basicConfig(level=logging.DEBUG)

parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument(
    "--chunk_size", type=int, default=100, help="Chunk size for CSV ingestion"
)
parser.add_argument(
    "--csv_path",
    type=str,
    nargs="+",
    default=["./data/original_data.csv"],
    help="Path to the CSV file(s)",
)
parser.add_argument("--host", type=str, default="localhost", help="Postgres host")
parser.add_argument("--port", type=int, default=5432, help="Postgres port")
parser.add_argument(
    "--database", type=str, default="postgres", help="Postgres database"
)
parser.add_argument("--user", type=str, default="postgres", help="Postgres user")
parser.add_argument(
    "--table_name", type=str, default="data", help="Postgres table name"
)

args = parser.parse_args()
for path in args.csv_path:
    logging.info(f"Ingesting {path}")
    runner = Runner(
        input=IngestCSV(chunk_size=args.chunk_size, path=path),
        output=PostgresTarget(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=os.environ["POSTGRES_PASSWORD"],
            table_name=args.table_name,
        ),
    )
    runner.run()
