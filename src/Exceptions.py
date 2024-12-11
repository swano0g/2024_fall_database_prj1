class DuplicateColumnDefError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: column definition is duplicated")

class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: primary key definition is duplicated")

class ReferenceTypeError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: foreign key references wrong type")

class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: foreign key references non primary key column")

class ReferenceExistenceError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: foreign key references non existing table or column")

class PrimaryKeyColumnDefError(Exception):
    def __init__(self, col_name):
        super().__init__(f"Create table has failed: cannot define non-existing column '{col_name}' as primary key")                       

class ForeignKeyColumnDefError(Exception):
    def __init__(self, col_name):
        super().__init__(f"Create table has failed: cannot define non-existing column '{col_name}' as foreign key")

class TableExistenceError(Exception):
    def __init__(self):
        super().__init__("Create table has failed: table with the same name already exists")
    
class CharLengthError(Exception):
    def __init__(self):
        super().__init__("Char length should be over 0")

class NoSuchTable(Exception):
    def __init__(self, command_name):
        super().__init__(f"{command_name} has failed: no such table")
                  
class DropReferencedTableError(Exception):
    def __init__(self, table_name):
        super().__init__(f"Drop table has failed: '{table_name}' is referenced by another table")

class SelectTableExistenceError(Exception):
    def __init__(self, table_name):
        super().__init__(f"Select has failed: '{table_name}' does not exist")
                                
                 
                                
                                
class InsertTypeMismatchError(Exception):
    def __init__(self):
        super().__init__("Insert has failed: types are not matched")


class InsertColumnExistenceError(Exception):
    def __init__(self, column_name):
        super().__init__(f"Insert has failed: '{column_name}' does not exist")


class InsertColumnNonNullableError(Exception):
    def __init__(self, column_name):
        super().__init__(f"Insert has failed: '{column_name}' is not nullable")


class SelectColumnResolveError(Exception):
    def __init__(self, column_name):
        super().__init__(f"Select has failed: fail to resolve '{column_name}'")





class IncomparableError(Exception):
    def __init__(self):
        super().__init__('Trying to compare incomparable columns or values')


                    

class TableNotSpecified(Exception):
    def __init__(self, clause_name):
        super().__init__(f"{clause_name} clause trying to reference tables which are not specified")         
        
class ColumnNotExist(Exception):
    def __init__(self, clause_name):
        super().__init__(f"{clause_name} clause trying to reference non existing column")
        
class AmbiguousReference(Exception):
    def __init__(self, clause_name):
        super().__init__(f"{clause_name} clause contains ambiguous column reference")   
                    
    
# new defined
# 참조하는 열과 참조되는 열의 개수 다를 때
class ReferenceColumnMatchError(Exception):
    def __init__(self):
        super().__init__("Number of referencing columns must match referenced columns.")
    

class DuplicatedColumnNameError(Exception):
    def __init__(self):
        super().__init__("Column name duplicated.")