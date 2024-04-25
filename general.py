from enum import Enum
from numpy.dtypes import ObjectDType,Int32DType,Int64DType,Float64DType,Float32DType

class SchemaDataTypes(Enum):
    object = str(ObjectDType)
    int32 = str(Int32DType)
    int64 = str(Int64DType)
    float32 = str(Float32DType)
    float64 = str(Float64DType)


class MessageTypes(Enum):
    INGEST = "INGEST"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"

class InterMessage:
    message_type: MessageTypes
    message: dict