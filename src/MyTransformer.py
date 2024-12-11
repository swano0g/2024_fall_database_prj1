from __future__ import annotations
from lark import Lark, UnexpectedInput, Transformer
from src.DatabaseHandler import DatabaseHandler
from src import Exceptions, RecordEvaluator
from datetime import date, datetime
import uuid

# MyTransformer class. lark 모듈의 Transformer 클래스를 상속받는다.
class MyTransformer(Transformer):
    def __init__(self, id, db_handler: DatabaseHandler):
        super().__init__()        
        self.id = id
        # 데이터베이스를 통해 값을 읽고 쓸 때 모두 db_handler를 거친다.
        self.db_handler = db_handler
    

    # create query에서 외래키 관련 조건을 메타데이터에 업데이트 해주는 함수
    def update_referenced_by(self, referencing_table, referenced_table, referencing_column, referenced_columns):
        metadata = self.db_handler.get_table_metadata(referenced_table)
        if metadata:
            tmp = {}
            tmp["referenced_columns"] = referenced_columns
            tmp["referencing_table"] = referencing_table
            tmp["referencing_column"] = referencing_column
            metadata["referenced_by"].append(tmp)
            self.db_handler.metadata_put(referenced_table, metadata)
            
    # drop table query에서 외래키 관련 메타데이터를 삭제해주는 함수.
    def delete_referenced_by(self, table_name, metadata):
        for fk_table in metadata["foreign_keys"]:
            target_table = fk_table["fk_ref_table"]
            target_metadata = self.db_handler.get_table_metadata(target_table)
            
            if target_metadata:
            # `referenced_by` 목록에서 삭제된 테이블과 관련된 항목 제거
                target_metadata["referenced_by"] = [
                    ref for ref in target_metadata["referenced_by"]
                    if ref["referencing_table"] != table_name or ref["referenced_columns"] != fk_table["fk_ref_columns"]
                ]
            
            self.db_handler.metadata_put(target_table, target_metadata)
    
    # 데이터를 프롬프트에 출력할 때 형식을 맞춰주는 함수.
    def prompt_out(self, headers, data):
        """
        Print header and data to prompt
        """
        
        # for i in range(len(data)):
        #     for j in range(len(data[i])):
        #         if not data[i][j]:
        #             data[i][j] = ""
                    
        L = len(data)
        if headers:
            widths = [len(header) for header in headers]
        else:
            widths = [20]
            
        for row in data:
            widths = [max(width, len(str(cell))) for width, cell in zip(widths, row)]
        widths = [(width + 9) // 10 * 10 for width in widths]
        
        separator = "-" * (sum(widths) + 10)
        format_string = " | ".join(f"{{:<{width}}}" for width in widths)
        
        print(separator)
        if headers:
            print(format_string.format(*headers))
        for row in data:
            print(format_string.format(*row))
        print(separator)
        
        
        if L == 1:
            print(f"{L} row in set")
        else:
            print(f"{L} rows in set")
    
    # desc, explain, describe query에서 공통적으로 사용.
    def _table_info_print(self, target_table):
        target_metadata = self.db_handler.get_table_metadata(target_table)
        
        L = len(target_metadata["column_order"])
        headers = ["COLUMN_NAME", "TYPE", "NULL", "KEY"]
        data = []
        
        for i in target_metadata["column_order"]:
            typ = target_metadata["columns"][i]["data_type"]
            nul = "N" if target_metadata["columns"][i]["not_null"] else "Y"
            key_p = True if i in target_metadata["primary_keys"] else False
            
            for j in target_metadata["foreign_keys"]:
                if i in j["fk_columns"]:
                    key_f = True
                    break
            else:
                key_f = False  
                
            if key_p and key_f:
                key = "PRI/FOR"
            elif (not key_p) and key_f:
                key = "FOR"
            elif key_p and (not key_f):
                key = "PRI"
            else:
                key = ""
                
            data.append([i, typ, nul, key])
        
        self.prompt_out(headers, data)
        
        
        # print("raw data:")
        # print(target_metadata)
        

    

    def table_data_parser(self, val, val_type, flag = False):
        '''
            string 타입인 데이터들을 메타데이터의 각 column의 정의에 따라 데이터 타입을 바꿔주는 함수. 
            flag는 DATE 타입을 string, date중 어떤 것으로 바꿀지 결정.
        '''
        if val == "NULL":
            return None
        
        if val_type == "INT":
            return int(val)
        elif val_type == "DATE":
            if flag:
                return val
            else:
                res = datetime.strptime(val, '%Y-%m-%d').date()
                return res
        else:
            res = val[1:-1]
            max_length = int(val_type[5:-1])
            if len(res) > max_length:
                res = res[:max_length]
                
            return res
        
    # 파싱트리에서 특정 토큰을 찾는 함수.
    def _find_tokens(self, data, token, upper = True, flag = False):
        """
        from data find value of token.
        if flag is True, return type of value too.
        """
        tmp = []
        types = []
        if data:
            columns = data.find_data(token)
            for k in columns:
                if upper:
                    tmp.append(k.children[0].value.upper())
                else:
                    tmp.append(k.children[0].value)        
                                
                if flag:
                    types.append(k.children[0].type.upper())
                
        if flag:
            return tmp, types
        else:
            return tmp
    
    # 아래의 함수들은 파싱 트리를 탐색하며 각각의 명령(create_table_query, select_query 등)을 만났을 때 수행할 코드를 정의한다.
    def create_table_query(self, items):
        """
        Metadata follows the format below
        {'column_order':    ['C1', 'C2', 'C3', 'C4'], 
         'columns':         {
                            'C1': {'data_type': 'INT', 'not_null': True}, 
                            'C2': {'data_type': 'INT', 'not_null': True}, 
                            'C3': {'data_type': 'INT', 'not_null': True}, 
                            'C4': {'data_type': 'DATE', 'not_null': False}}, 
         'primary_keys':    ['C1'], 
         'foreign_keys':    [
                            {'fk_columns': ['C2'], 'fk_ref_table': 'table_name', 'fk_ref_columns': ['C2']}, 
                            {'fk_columns': ['C3'], 'fk_ref_table': 'table_name', 'fk_ref_columns': ['C3']}], 
         'referenced_by':   [{'referenced_columns': ['C1'], 'referencing_table': 'OTHER_TABLE', 'referencing_column': ['OTHER_TABLE_COLUMN']}]
         }
        """
        
        metadata = {}
        col_order = []
        columns = {}
        primary_keys = []
        foreign_keys = []
        referenced_by = []
        
        
        column_def_iter = items[3].find_data("column_definition")
        
        primary_key_def_iter = items[3].find_data("primary_key_constraint")
        foregin_key_def_iter = items[3].find_data("referential_constraint")
        
        
        table_name = items[2].children[0].value.upper()
        
        if self.db_handler.table_exist(table_name):
            raise Exceptions.TableExistenceError
        
        
        # column informations
        for i in column_def_iter:
            
            col_name = i.children[0].children[0].value.upper()
            
            d_type = ""
            for j in i.children[1].children:
                d_type += j.upper()
                
            if d_type.startswith("CHAR"):
                n = int(d_type[5:-1])
                if n < 1:
                    raise Exceptions.CharLengthError
                        
            if (i.children[2] and (i.children[2].upper() == "NOT")):
                not_null = True
            else:
                not_null = False   
            
            col_order.append(col_name)
            
            if col_name in columns:
                raise Exceptions.DuplicateColumnDefError
            else:
                columns[f"{col_name}"] = {"data_type": d_type, "not_null": not_null}
        
        
        
        # primary key 
        for i, j in enumerate(primary_key_def_iter):
            if i > 0:
                raise Exceptions.DuplicatePrimaryKeyDefError
            
            primary_keys = self._find_tokens(j.children[2], "column_name")

        for i in primary_keys:
            if i not in columns:
                raise Exceptions.PrimaryKeyColumnDefError(i)
            
        for i in primary_keys:
            columns[i]["not_null"] = True


        # foreign key
        for i in foregin_key_def_iter:
            temp = {}
            temp["fk_columns"] = self._find_tokens(i.children[2], "column_name")
            temp["fk_ref_table"] = i.children[4].children[0].value.upper()
            temp["fk_ref_columns"] = self._find_tokens(i.children[5], "column_name")
 
            # 제약조건 확인
            ref_metadata = self.db_handler.get_table_metadata(temp["fk_ref_table"])
            
            if len(temp["fk_columns"]) != len(temp["fk_ref_columns"]):
                raise Exceptions.ReferenceColumnMatchError
            
            
            # 자기 자신을 참조하는 경우
            if temp["fk_ref_table"] == table_name:
                if len(temp["fk_ref_columns"]) != len(primary_keys):
                    raise Exceptions.ReferenceNonPrimaryKeyError
                
                for j, k in zip(temp["fk_columns"], temp["fk_ref_columns"]):
                    if j not in columns:
                        raise Exceptions.ForeignKeyColumnDefError(j)
                    
                    if k not in primary_keys:
                        if k in columns:
                            raise Exceptions.ReferenceNonPrimaryKeyError
                        
                        else:
                            raise Exceptions.ReferenceExistenceError
                    
                    if columns[j]["data_type"] != columns[k]["data_type"]:
                        raise Exceptions.ReferenceTypeError
                
                foreign_keys.append(temp)
            
            
            elif ref_metadata:
                if len(temp["fk_ref_columns"]) != len(ref_metadata["primary_keys"]):
                    raise Exceptions.ReferenceNonPrimaryKeyError
                
                for j, k in zip(temp["fk_columns"], temp["fk_ref_columns"]):
                    if j not in columns:
                        raise Exceptions.ForeignKeyColumnDefError(j)
                    
                    if k not in ref_metadata["primary_keys"]:
                        if k in ref_metadata["columns"]:
                            raise Exceptions.ReferenceNonPrimaryKeyError
                        
                        else:
                            raise Exceptions.ReferenceExistenceError
                    
                    if columns[j]["data_type"] != ref_metadata["columns"][k]["data_type"]:
                        raise Exceptions.ReferenceTypeError
         
                foreign_keys.append(temp)
                
            else:
                raise Exceptions.ReferenceExistenceError
        # 모든 제약조건 확인 종료
        
        for i in foreign_keys:
            if i["fk_ref_table"] != table_name:
                self.update_referenced_by(table_name, i["fk_ref_table"], i["fk_columns"], i["fk_ref_columns"])
            else:
                referenced_by.append({"referenced_columns": i["fk_ref_columns"], "referencing_table": i["fk_ref_table"], "referencing_column":  i["fk_columns"]})
                    
    
        metadata["column_order"] = col_order
        metadata["columns"] = columns
        metadata["primary_keys"] = primary_keys
        metadata["foreign_keys"] = foreign_keys
        metadata["referenced_by"] = referenced_by
        
        self.db_handler.open_table(table_name)
        self.db_handler.metadata_put(table_name, metadata)
        print(f"DB_{self.id}> '{table_name}' table is created")
    
    
    def _cartesian_product(self, tables):
        result = tables[0]
        if len(tables) == 1:
            return result
        
        for table in tables[1:]:
            temp = []
            for record1 in result:
                for record2 in table:
                    temp.append(record1 + record2)
            result = temp
        return result
      
    def select_query(self, items):
        select_clause = items[1].children
        from_clause = list(items[2].children[0].find_data("referred_table"))    
        join_clause = None if not items[2].children[1] else items[2].children[1]
        where_clause = None if not items[2].children[2] else items[2].children[2]
        order_by_clause = None if not items[2].children[3] else items[2].children[3]

        select_info = []
        
        from_info = []
        join_info = []
        join_conditions = []
        
        order_by_info = []
        
        for i in from_clause:
            a = i.children
            table_name = a[0].children[0].value.upper()
            table_meta = self.db_handler.get_table_metadata(table_name)
            if not table_meta:
                raise Exceptions.SelectTableExistenceError(table_name)
            
            from_info.append(table_name)
        
        
        if join_clause:
            for join_expr in join_clause.find_data('join_expr'):
                table_name = join_expr.children[1].children[0].value.upper()
                join_condition = join_expr.children[3].children[0]
            
                if not self.db_handler.table_exist(table_name):
                    raise Exceptions.SelectTableExistenceError(table_name)
                
                join_info.append(table_name)  
                join_conditions.append(join_condition)          

        if not select_clause:
            for table_name in (from_info + join_info):
                for col_name in self.db_handler.get_table_metadata(table_name)["column_order"]:
                    select_info.append(f"{table_name}.{col_name}")
        else:
            for select_condition in select_clause:
                a = select_condition.children
                table_name = None if not a[0] else a[0].children[0].value.upper()
                column_name = None if not a[1] else a[1].children[0].value.upper()
                
                
                if table_name:
                    if (table_name not in from_info) and (table_name not in join_info):
                        raise Exceptions.SelectColumnResolveError(column_name)
                    elif column_name not in self.db_handler.get_table_metadata(table_name)["columns"]:
                        raise Exceptions.SelectColumnResolveError(column_name)
                    
                else:
                    for key in (from_info + join_info):
                        value = self.db_handler.get_table_metadata(key)
                        if column_name in value["columns"]:
                            if table_name:
                                raise Exceptions.SelectColumnResolveError(column_name)
                            table_name = key
                    if not table_name:
                        raise Exceptions.SelectColumnResolveError(column_name)
                select_info.append(table_name + "." + column_name)
            

        if order_by_clause:
            table_name = None if not order_by_clause.children[2] else order_by_clause.children[2].children[0].value.upper()
            column_name = order_by_clause.children[3].children[0].value.upper()
            order = "ASC" if not order_by_clause.children[4] else order_by_clause.children[4].value.upper()
            
            if table_name:
                if (table_name not in from_info) and (table_name not in join_info):
                    raise Exceptions.ColumnNotExist("Order by")
                elif column_name not in self.db_handler.get_table_metadata(table_name)["columns"]:
                    raise Exceptions.ColumnNotExist("Order by")
            else:
                for key in (from_info + join_info):
                    value = self.db_handler.get_table_metadata(key)
                    if column_name in value["columns"]:
                        if table_name:
                            raise Exceptions.AmbiguousReference("Order by")
                        table_name = key
                if not table_name:
                    raise Exceptions.ColumnNotExist("Order by")
                
            order_by_info.append(table_name + "." + column_name)
            order_by_info.append(order)
        ###
        
        
        
        # FROM operation, cartesian product
        tmp = []
        for t in from_info:
            tmp.append(self.db_handler.table_get_all(t, flag=False))
            
        result_table = self._cartesian_product(tmp)
        result_column = []
        
        for table_name in from_info:
            for column_name in self.db_handler.get_table_metadata(table_name)["column_order"]:
                result_column.append(table_name + "." + column_name)
        
        # from연산 완료 결과-> from_where_result
        
        
        # JOIN operation. do cartesian product and check ON condition
        for join_table, join_condition in zip(join_info, join_conditions):
            result_table = self._cartesian_product([result_table, self.db_handler.table_get_all(join_table, flag=False)])
            from_info.append(join_table)
            for column_name in self.db_handler.get_table_metadata(join_table)["column_order"]:
                result_column.append(join_table + "." + column_name)
            recordEvaluator = RecordEvaluator.RecordEvaluator(result_column, from_info, join_condition, "Join")
            result_table = list(filter(recordEvaluator.evaluate_record, result_table))
        # JOIN END
        
        
        # WHERE operation
        if where_clause:
            recordEvaluator = RecordEvaluator.RecordEvaluator(result_column, from_info, where_clause, "Where")
            result_table = list(filter(recordEvaluator.evaluate_record, result_table))

        # WHERE operation end
        
        # order by
        
        if order_by_info:
            sort_idx = result_column.index(order_by_info[0])
            result_table.sort(key=lambda x: x[sort_idx] if x[sort_idx] is not None else float('-inf'), reverse=(order.upper() == 'DESC'))
    
        # order by done
        
    
        # Project operation
        column_indices = []
        for i in select_info:
            column_indices.append(result_column.index(i))
            
        result_table = [[row[i] for i in column_indices] for row in result_table]
        
        # project done
        
        ### DONE
        column_names = [name.split('.')[1] for name in select_info]
        duplicates = set(col for col in column_names if column_names.count(col) > 1)

        # generate header
        column_header = []
        for full_name in select_info:
            table_name, col_name = full_name.split('.')
            if col_name in duplicates:
                column_header.append(full_name) 
            else:
                column_header.append(col_name) 
        
        for i in range(len(result_table)):
            for j in range(len(result_table[i])):
                if type(result_table[i][j]) == date:
                    result_table[i][j] = result_table[i][j].strftime("%Y-%m-%d")
                if not result_table[i][j]:
                    result_table[i][j] = "NULL"
        
        self.prompt_out(column_header, result_table)
        
        
        
        
    def insert_query(self, items):  
        table_name = items[2].children[0].value.upper()
        col_name = self._find_tokens(items[3], "column_name")   
        values, values_type = self._find_tokens(items[5], "value", upper=False, flag=True)
        table_metadata = self.db_handler.get_table_metadata(table_name)
        
        if not col_name:
            col_name = table_metadata["column_order"]
        
        
        ## 제약조건 확인 시작
        # 삽입할 테이블이 존재하지 않을 경우
        if not table_metadata:
            raise Exceptions.NoSuchTable("insert")
        
        
        # 지정된 컬럼과 값의 개수가 다른 경우
        # 컬럼을 명시하지 않았는데, 입력 값 개수와 해당 테이블의 attribute 수가 다른 경우
        if len(col_name) != len(values):
            raise Exceptions.InsertTypeMismatchError
        else:
            if len(set(col_name)) != len(values):
                raise Exceptions.DuplicatedColumnNameError
        
        for col in col_name:
            # 존재하지 않는 column에 값을 삽입하는 경우
            if col not in table_metadata["columns"]:
                raise Exceptions.InsertColumnExistenceError(col) 

        inserting_value = self._inserthelper(col_name, values, values_type, table_metadata["column_order"], table_metadata)
        
        key_value = str(uuid.uuid4())
        self.db_handler.table_put(table_name, key_value, inserting_value)
        print(f"DB_{self.id}> 1 row inserted")
        
        
    # 테이블에 insert 할 때 value 값 리스트를 만드는 함수.
    def _inserthelper(self, col_name: list, values: list, values_type: list, col_order: list, table_metadata) -> list[int | str | datetime.date]:
        result = [None for _ in range(len(col_order))]

        for idx in range(len(col_order)):
            for i, col in enumerate(col_name):
                if col_order[idx] == col:
                    
                    if (values_type[i] == "STR" and table_metadata["columns"][col]["data_type"][0] == "C"):
                        result[idx] = self.table_data_parser(values[i], table_metadata["columns"][col]["data_type"], True)
                    
                    elif (values_type[i] == table_metadata["columns"][col]["data_type"]):
                        result[idx] = self.table_data_parser(values[i], values_type[i], True)

                    # null 값을 삽입하려는 경우
                    elif (values_type[i] == "NULL"):
                        if (table_metadata["columns"][col]["not_null"]):
                            raise Exceptions.InsertColumnNonNullableError(col_order[idx])
                        else:
                            pass
                    
                        
                    
                    # 지정된 컬럼과 값의 타입이 맞지 않는 경우
                    else:
                        raise Exceptions.InsertTypeMismatchError
                    
                    break
                        
            else:
                # null 여부
                if table_metadata["columns"][col_order[idx]]["not_null"]:
                    raise Exceptions.InsertColumnNonNullableError(col_order[idx])
        return result

        
    def drop_table_query(self, items):
        target_table = items[2].children[0].upper()
        target_metadata = self.db_handler.get_table_metadata(target_table)
        
        if target_metadata:
            if target_metadata["referenced_by"]:
                for i in target_metadata["referenced_by"]:
                    if i["referencing_table"] != target_table:
                        raise Exceptions.DropReferencedTableError(target_table)
            
            # drop possible
            self.delete_referenced_by(target_table, target_metadata)
            self.db_handler.delete_table(target_table)
            self.db_handler.metadata_delete(target_table)
            print(f"DB_{self.id}> '{target_table}' table is dropped")
            return
            
        else:
            raise Exceptions.NoSuchTable("drop table")
    
    def explain_query(self, items):
        target_table = items[1].children[0].upper()
        if not self.db_handler.table_exist(target_table):
            raise Exceptions.NoSuchTable("explain")
            
        self._table_info_print(target_table)
        
    def describe_query(self, items):
        target_table = items[1].children[0].upper()
        if not self.db_handler.table_exist(target_table):
             raise Exceptions.NoSuchTable("describe")
        self._table_info_print(target_table)
        
    def desc_query(self, items):
        target_table = items[1].children[0].upper()
        if not self.db_handler.table_exist(target_table):
            raise Exceptions.NoSuchTable("desc")
        self._table_info_print(target_table)
        
    
    def show_tables_query(self, items):
        data = self.db_handler.get_table_list()
        self.prompt_out([], data)
        
    
    
    def delete_query(self, items):
        table_name = items[2].children[0].value.upper()
        table_metadata = self.db_handler.get_table_metadata(table_name)
        
        if not table_metadata:
            raise Exceptions.NoSuchTable("delete")
        
        meta_column_name  = [table_name + "." + item for item in table_metadata["column_order"]]
        
        where_clause = items[3]
    
        if not where_clause:
            deleted_count = self.db_handler.table_delete_all(table_name)
        
        else:
            records = self.db_handler.table_get_all(table_name)
            table_list = [table_name]
            whereEvaluator = RecordEvaluator.RecordEvaluator(meta_column_name, table_list, where_clause, "Where")
            
            delete_list = []
            for key, record in records:
                if whereEvaluator.evaluate_record(record):
                    delete_list.append(key)

            deleted_count = len(delete_list)
            
            for key in delete_list:
                self.db_handler.table_delete(table_name, key)
        
        
        
        if deleted_count == 1:
            print(f"DB_{self.id}> 1 row deleted")
        else:
            print(f"DB_{self.id}> {deleted_count} rows deleted")

    
    def update_tables_query(self, items):
        print(f"DB_{self.id}> '{items[0]}' requested")
    
    def EXIT(self, items):
        self.db_handler.close()
        exit()