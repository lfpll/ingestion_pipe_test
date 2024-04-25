# Validate if anything will be converted to null
from abc import ABC

class TargetBase(ABC):

    def push(self):
        pass

    def convert_changes_of_schema(self):
        pass
    
    def get_schema(self):
        pass
