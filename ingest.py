from abc import ABC
from dataclasses import dataclass
from typing import Generator, Union
import pandas as pd
from general import SchemaDataTypes

@dataclass
class IngestOutput:
    data: str
    schema: dict[str,str]

class ReaderBase(ABC):

    def __init__(self, 
                 chunk_size: int) -> None:
        self.chunk_size = chunk_size    
    
    def _format_schema_to_dict():
        pass

    def load_data() -> Generator[IngestOutput]:
        pass

    def output_data_as_jsonl():
        pass

class IngestCSV(ReaderBase):

    def __init__(self, 
                chunk_size: int,
                has_header: bool) -> None:
        self.has_header = has_header
        self.chunk_size = chunk_size
        super().__init__(chunk_size)

    def _format_schema_to_dict(self, df:pd.DataFrame) -> dict[str,SchemaDataTypes]:
        return {column: str(df[column].dtype) for column in df.columns}

    def output_data_as_jsonl(self, df: pd.DataFrame):
        return df.to_json(orient='records', lines=True)
    
    def load_data(self,file_path: str) -> Generator[IngestOutput]:
        reader = pd.read_csv(file_path, chunksize=self.chunk_size, header=self.has_header)
        for chunk in reader:
            yield {
                "data": self.output_data_as_jsonl(chunk),
                "schema": self._format_schema_to_dict(chunk)
            }

