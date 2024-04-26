from abc import ABC, abstractmethod
from typing import Any, Generator, TypedDict

import pandas as pd


class IngestOutput(TypedDict):
    data: str
    schema: dict[str, str]


class ReaderBase(ABC):
    def __init__(self, chunk_size: int) -> None:
        self.chunk_size = chunk_size

    @abstractmethod
    def _format_schema_to_dict(self, df: pd.DataFrame) -> dict[str, str]:
        """Formats and returns the schema as a dictionary."""
        pass

    @abstractmethod
    def output_data_to_dict(self, df: pd.DataFrame) -> dict[str, Any]:
        """Converts data to a dictionary."""
        pass

    @abstractmethod
    def load_data(self) -> Generator[IngestOutput, None, None]:
        """Loads data in chunks and yields it."""
        pass


class IngestCSV(ReaderBase):
    def __init__(self, chunk_size: int, path: str) -> None:
        self.path = path
        self.chunk_size = chunk_size

        super().__init__(chunk_size)

    def _format_schema_to_dict(self, df: pd.DataFrame) -> dict[str, str]:
        return {k: str(d) for k, d in df.dtypes.to_dict().items()}

    def output_data_to_dict(self, df: pd.DataFrame):
        return df.to_dict(orient="records")

    def load_data(self) -> Generator[IngestOutput, None, None]:
        reader = pd.read_csv(self.path, chunksize=self.chunk_size, header=0)
        for chunk in reader:
            yield {
                "data": self.output_data_to_dict(chunk),
                "schema": self._format_schema_to_dict(chunk),
            }
