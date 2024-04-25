# 1. Get the first schema
# 2. Get the second schema
# 3. Validate if the schema are compatible
# 4. If they are compatible do nothing
# 5. If they are incompatible check if the operations are possible
# 6. If the operations are possible send then through
from api_extractor.general import SchemaDataTypes
from output import TargetBase
from ingest import ReaderBase
from general import InterMessage

class Modification:
    new_dtype: SchemaDataTypes
    old_dtype: SchemaDataTypes

class SchemaChanges:
    adds: list[str] | list = []
    modifications: dict[str, Modification] = dict()


class Runner:
    allowable_conversions = {
        'int32': {'int64', 'float32', 'float64', 'object'},
        'int64': {'float64', 'object'},
        'float32': {'float64', 'object'},
        'float64': {'object'},
        # 'object': {'int32', 'int64', 'float32', 'float64', 'string', 'object'},
        'string': {'object'},
        # Initally True and False can become more categories
        # e.g: True,False => 1,2 => 1,2,3,4
        'bool': {'object'}
    }

    def __init__(self, input: ReaderBase, output: TargetBase):
        self.input = input
        self.output = output

    def is_modifications_possible(self, old_dtype: SchemaDataTypes, new_dtype: SchemaDataTypes) -> bool:
        return new_dtype in self.allowable_conversions[old_dtype]

    def get_schema_changes(self,
                        input_schema: dict[str, SchemaDataTypes],
                        output_schema: dict[str, SchemaDataTypes]) -> SchemaChanges:

        def is_column_new(col_name: str, schema: dict[str, SchemaDataTypes]) -> bool:
            return col_name not in schema
        
        def is_column_dtype_changed(input_dtype: SchemaDataTypes,
                                output_dtype: SchemaDataTypes) -> bool:
            return input_dtype != output_dtype
        
        def get_column_dtype(col_name: str,
                            schema: dict[str, SchemaDataTypes]) -> SchemaDataTypes:
            return schema[col_name]
    
        changes = SchemaChanges()
        for column, dtype in input_schema.items():
            if is_column_new(column, output_schema):
                changes.adds.append(column)
            elif is_column_dtype_changed(dtype,
                                        get_column_dtype(column, output_schema)):
                old_tdype = get_column_dtype(column, output_schema)
                changes.modifications[column] = Modification(
                    new_dtype=dtype,
                    old_dtype=old_tdype
                )

        return changes

    def run(self):
        output_schema = self.output.get_schema()
        for data in self.input.load_data():
            input_schema = data['schema']
            
            schema_changes = self.get_schema_changes(
                input_schema, output_schema)
            
            if schema_changes.adds or schema_changes.modifications:
                is_valid_change = all([self.is_modifications_possible(m.new_dtype,m.old_dtype) 
                                for _,m in schema_changes.modifications.items()])
                
                print(schema_changes)  
                if is_valid_change:
                    self.output.push(InterMessage(
                        message_type="SCHEMA_CHANGE",
                        message=schema_changes
                    ))
                else:
                    raise ValueError("Incompatible schema")

            self.output.push(InterMessage(
                message_type="INGEST",
                message=data
            ))
