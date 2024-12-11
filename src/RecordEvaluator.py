from dataclasses import dataclass
from datetime import datetime
from src import Exceptions

@dataclass
class ColumnInfo:
    """컬럼 정보"""
    full_name: str      # 'BOOKS.BOOK_ID'
    table_name: str     # 'BOOKS'
    column_name: str    # 'BOOK_ID'
    index: int          # 레코드에서의 위치

    @classmethod
    def parse_column_name(cls, full_name: str) -> tuple:
        parts = full_name.split('.')
        return parts[0], parts[1]


class RecordEvaluator:
    def __init__(self, column_names, table_list, condition_tree, clause_name):
        self.columns = {}
        self.table_list = table_list
        self.condition_tree = condition_tree
        self.clause_name = clause_name
        
        for i, full_name in enumerate(column_names):
            table_name, column_name = ColumnInfo.parse_column_name(full_name)
            column_info = ColumnInfo(
                full_name=full_name,
                table_name=table_name,
                column_name=column_name,
                index=i
            )
            self.columns[full_name] = column_info


    def evaluate_record(self, record) -> bool:
        """where condition evaluate"""
        return self._evaluate_node(self.condition_tree, record)



    def _evaluate_node(self, node, record) -> bool:
        if not hasattr(node, 'data'):
            return True
        
        node_type = node.data
        
        if node_type == 'where_clause':
            return self._evaluate_node(node.children[1], record)
            
        elif node_type == 'boolean_expr':
            return self._evaluate_boolean_expr(node.children, record)
            
        elif node_type == 'boolean_term':
            return self._evaluate_boolean_term(node.children, record)
            
        elif node_type == 'boolean_factor':
            has_not = len(node.children) > 1 and str(node.children[0]).upper() == 'NOT'
            result = self._evaluate_node(node.children[-1], record)
            return not result if has_not else result
            
        elif node_type == 'boolean_test':
            return self._evaluate_node(node.children[0], record)
            
        elif node_type == 'parenthesized_boolean_expr':
            return self._evaluate_node(node.children[1], record)
            
        elif node_type == 'predicate':
            return self._evaluate_node(node.children[0], record)
            
        elif node_type == 'comparison_predicate':
            return self._evaluate_comparison(node.children, record)
            
        elif node_type == 'null_predicate':
            return self._evaluate_null_predicate(node.children, record)
        
        return False



    def _evaluate_boolean_expr(self, children, record) -> bool:
        """or"""
        result = self._evaluate_node(children[0], record)
        for i in range(2, len(children), 2):
            result = result or self._evaluate_node(children[i], record)
        
            if result:
                return True
            
        return result
   

    def _evaluate_boolean_term(self, children, record) -> bool:
        """and"""
        result = self._evaluate_node(children[0], record)
        for i in range(2, len(children), 2):
            result = result and self._evaluate_node(children[i], record)
           
            if not result:
                return False
           
        return result


    def _evaluate_comparison(self, children, record) -> bool:
        """condition evaluate"""
       
        left_val = self._get_operand_value(children[0], record)
        op = str(children[1].children[0])
        right_val = self._get_operand_value(children[2], record)
       
       
        if left_val is None or right_val is None:
            return False
        
        if type(left_val) != type(right_val):
            raise Exceptions.IncomparableError(self.clause_name)
        
        if (type(left_val) == str and (op != '=' and op != '!=')):
            raise Exceptions.IncomparableError(self.clause_name)
        
        if op == '=':
            return left_val == right_val
        elif op == '<':
            return left_val < right_val
        elif op == '<=':
            return left_val <= right_val
        elif op == '>':
            return left_val > right_val
        elif op == '>=':
            return left_val >= right_val
        elif op == '!=':
            return left_val != right_val
       
        return False


    def _get_operand_value(self, node, record):
        """피연산자 값 추출"""
        
        children = node.children
    
        # column_name
        if len(children) == 2:
            table_name = None
            
            if not children[0]:  # table_name이 None인 경우
                column_name = children[1].children[0].value.upper()
                
                for full in self.columns.keys():
                    front, end = full.split(".")
                    if end == column_name:
                        if table_name:
                            raise Exceptions.AmbiguousReference(self.clause_name)
                        table_name = front

                full_name = f"{table_name}.{column_name}"
                
            else:  # table_name이 있는 경우
                table_name = children[0].children[0].value.upper()
                column_name = children[1].children[0].value.upper()
                full_name = f"{table_name}.{column_name}"
                
                if table_name not in self.table_list:
                    raise Exceptions.TableNotSpecified(self.clause_name)
                
            col_info = self.columns.get(full_name)
            
            # column 존재
            if col_info:
                value = record[col_info.index]
                return value
            
            # column 존재 x
            else:
                raise Exceptions.ColumnNotExist(self.clause_name)
    
        
        # comparable value
        elif len(children) == 1:
            return self._parse_literal(children[0].children[0])
        
        return None


    def _evaluate_null_predicate(self, children, record) -> bool:
        """NULL 비교 평가"""

        table_name = None
        if children[0] is None:  # table_name이 None인 경우
            column_name = children[1].children[0].value.upper()
            
            for full in self.columns.keys():
                    front, end = full.split(".")
                    if end == column_name:
                        if table_name:
                            raise Exceptions.AmbiguousReference(self.clause_name)
                        table_name = front
            
            full_name = f"{table_name}.{column_name}"
        else:  # table_name이 있는 경우
            table_name = children[0].children[0].value.upper()
            column_name = children[1].children[0].value.upper()
            full_name = f"{table_name}.{column_name}"
            
            if table_name not in self.table_list:
                    raise Exceptions.TableNotSpecified(self.clause_name)
        
        col_info = self.columns.get(full_name)
        if not col_info:
            raise Exceptions.ColumnNotExist(self.clause_name)
        
        # null_operation: [IS, NOT?, NULL]
        null_op = children[2].children
        is_not = null_op[1] is not None
        value = record[col_info.index]
        return (value is None) != is_not


    def _parse_literal(self, token):
        """리터럴 값 파싱"""
        token_type = token.type
        value = str(token)
       
        if token_type == 'INT':
            return int(value)
        elif token_type == 'STR':
            return value.strip("'\"")
        elif token_type == 'DATE':
            return datetime.strptime(value, '%Y-%m-%d').date()
        
        return value
