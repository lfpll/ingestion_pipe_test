from logging import getLogger

from ingestion.models import DataMessage, Modification, SchemaChanges
from ingestion.source import ReaderBase
from ingestion.target import TargetBase

logger = getLogger(__name__)

COMPATIBILITY_MAP = {
    "int32": {"bool"},
    "int64": {"int32", "bool"},
    "float32": {"int32", "int64", "bool"},
    "float64": {"int32", "int64", "float32", "bool"},
    "object": {"int32", "int64", "float32", "float64", "string", "bool"},
}


class Runner:
    def __init__(self, input: ReaderBase, output: TargetBase):
        self.message = input
        self.output = output

    def is_modifications_required(self, old_dtype: str, new_dtype: str) -> bool:
        return old_dtype != new_dtype and new_dtype not in COMPATIBILITY_MAP[old_dtype]

    def get_schema_changes(
        self,
        input_schema: dict[str, str],
        output_schema: dict[str, str],
    ) -> SchemaChanges:
        def is_column_new(col_name: str, schema: dict[str, str]) -> bool:
            return col_name not in schema

        def is_conversion_needed(new_dtype, old_dtype):
            return (
                new_dtype != old_dtype and new_dtype not in COMPATIBILITY_MAP[old_dtype]
            )

        def is_conversion_possible(new_dtype, old_dtype):
            return old_dtype in COMPATIBILITY_MAP[new_dtype]

        def get_column_dtype(col_name: str, schema: dict[str, str]) -> str:
            return schema[col_name]

        changes = SchemaChanges()
        for column, new_dtype in input_schema.items():
            if is_column_new(column, output_schema):
                logger.info(f"Column {column} will be added with data type {new_dtype}")
                changes.adds[column] = new_dtype

            # TODO: improve this part
            elif is_conversion_needed(
                new_dtype, old_dtype := get_column_dtype(column, output_schema)
            ):
                if not is_conversion_possible(new_dtype, old_dtype):
                    logger.error(f"Column {column} has incompatible data types")
                    raise ValueError(f"Column {column} has incompatible data types")

                changes.modifications[column] = Modification(
                    new_dtype=new_dtype, old_dtype=old_dtype
                )
                logger.info(
                    f"Column {column} will be modified from {old_dtype} to {new_dtype}"
                )

        return changes

    def run(self):
        i = 0
        for message in self.message.load_data():
            i += 1
            logger.info(f"Processing message {i}")
            input_schema = message["schema"]
            data = message["data"]

            # TODO: improve this for table creation
            # This should work agnostically for any target
            breakpoint()
            if not self.output.table_exists():
                self.output.push(DataMessage("INGEST", data))

            output_schema = self.output.get_schema()
            schema_changes = self.get_schema_changes(input_schema, output_schema)

            if schema_changes.adds or schema_changes.modifications:
                self.output.push(DataMessage("SCHEMA_CHANGE", schema_changes))

            self.output.push(DataMessage("INGEST", data))
