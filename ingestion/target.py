# Validate if anything will be converted to null
from abc import ABC, abstractmethod
from logging import getLogger

import pandas as pd
import sqlalchemy

from ingestion.models import DataMessage, MessageTypes, SchemaMessage

logger = getLogger(__name__)


class TargetBase(ABC):
    @abstractmethod
    def push(self, message: DataMessage):
        pass

    @abstractmethod
    def execute_changes(self, message: SchemaMessage):
        pass

    @abstractmethod
    def get_schema(self):
        pass


class PostgresTarget(TargetBase):
    pandas_to_postgres_types = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "int16": "SMALLINT",
        "int8": "SMALLINT",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "object": "TEXT",
        "string": "TEXT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "timedelta[ns]": "INTERVAL",
        "category": "TEXT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
    }

    _connection = None

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        table_name: str,
    ):
        self.engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        self.table_name = table_name

    def execute_changes(self, message: SchemaMessage):
        changes = message.data

        for column, dtype in changes.adds.items():
            dtype = self.pandas_to_postgres_types[dtype]
            logger.debug(f"Adding column {column} with data type {dtype}")
            query = sqlalchemy.text(
                f"ALTER TABLE {self.table_name} ADD COLUMN {column} {dtype};"
            )
            self.conn.execute(query)
        for column, modification in changes.modifications.items():
            old_dtype = self.pandas_to_postgres_types[modification.old_dtype]
            new_dtype = self.pandas_to_postgres_types[modification.new_dtype]
            logger.debug(
                f"Alter column {column} with data type {old_dtype} to {new_dtype}"
            )
            query = sqlalchemy.text(
                f"ALTER TABLE {self.table_name} ALTER COLUMN {column} TYPE {new_dtype};"
            )
            self.conn.execute(query)

    def get_schema(self):
        schema = {
            k: str(d)
            for k, d in pd.read_sql(
                f"SELECT * FROM {self.table_name} LIMIT 1", self.conn
            )
            .dtypes.to_dict()
            .items()
        }
        return schema

    def is_conn_alive(self):
        return self._connection.closed

    @property
    def conn(self):
        if self._connection is None or self.is_conn_alive():
            self._connection = self.engine.connect()
        return self._connection

    def table_exists(self):
        return self.engine.dialect.has_table(self.conn, self.table_name)

    def load_data(self, message: DataMessage):
        df = pd.DataFrame(message.data)
        df.to_sql(self.table_name, self.conn, if_exists="append", index=False)
        self.conn.commit()

    def push(self, message: DataMessage | SchemaMessage):
        match message.message_type:
            case MessageTypes.SCHEMA_CHANGE.value:
                logger.info("Executing schema changes")
                self.execute_changes(message)
            case MessageTypes.INGEST.value:
                logger.info("Loading data")
                self.load_data(message)
            case _:
                raise ValueError(f"Invalid message type {message.message_type}")
