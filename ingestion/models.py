from dataclasses import dataclass
from enum import Enum


class MessageTypes(Enum):
    INGEST = "INGEST"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"


@dataclass
class Modification:
    new_dtype: str
    old_dtype: str


class SchemaChanges:
    adds: dict[str, str] = dict()
    modifications: dict[str, Modification] = dict()


@dataclass
class DataMessage:
    message_type: MessageTypes
    data: dict


class SchemaMessage:
    message_type: MessageTypes
    data: SchemaChanges
