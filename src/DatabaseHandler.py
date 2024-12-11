import json
from datetime import date, datetime
from berkeleydb import db

class DatabaseHandler():
    def __init__(self, database, env_path="DB", db_file="my_database.db"):
        self.db_file = db_file   # 데이터베이스 파일 이름
        self.env_path = env_path # 데이터베이스 파일 경로
        
        self.env = database        
        self.meta_db = db.DB(self.env)   # 메타데이터가 저장되는 테이블
        self.meta_db.open(db_file, "metadata", db.DB_HASH, db.DB_CREATE)

        self.tables = {}
        self.__restore_tables()
    
    
    def __restore_tables(self):
        """database startup"""
        # metadata 테이블을 순회해 데이터베이스에 저장되어 있는 테이블들을 모두 open 상태로 만듦
        cursor = self.meta_db.cursor()
        
        while record := cursor.next():
            key, _ = record
            table_name = key.decode()
            self.open_table(table_name)

        cursor.close()

    # 프로그램 종료 시 테이블들 안전하게 close
    def close(self):
        for table_db in self.tables.values():
            table_db.close()
        self.meta_db.close()
        self.env.close()
    
    # DBEnv에 새로운 테이블 생성
    def open_table(self, table_name):
        if table_name in self.tables:
            return 0
        
        table_db = db.DB(self.env)
        table_db.open("my_database.db", table_name, db.DB_HASH, db.DB_CREATE)
        self.tables[table_name] = table_db
        return 1
    
    # 테이블 삭제
    def delete_table(self, table_name):
        if table_name not in self.tables:
            return 0
        
        self.tables[table_name].close()
        del self.tables[table_name] 
        
        self.env.dbremove(self.db_file ,table_name)
    
    # 테이블의 메타데이터 불러오는 함수
    def get_table_metadata(self, table_name) -> dict:
        """from table name return metadata"""   
        metadata = self.meta_db.get(table_name.encode())
        if metadata:
            return json.loads(metadata.decode())
        return None
    
    
    def metadata_put(self, key, data):
        self.meta_db.put(key.encode(), json.dumps(data).encode())
    
    def metadata_delete(self, key):
        self.meta_db.delete(key.encode())
    
    #
    def table_put(self, target_table, key, data):
        self.tables[target_table].put(key.encode(), json.dumps(data).encode())
    
    def table_delete(self, target_table, key):
        self.tables[target_table].delete(key.encode())
    
    def table_delete_all(self, target_table):
        """delete every record in table"""
        deleted_count = self.tables[target_table].truncate()
        return deleted_count
        
    # 레코드 전체 순회
    def table_get_all(self, target_table, flag = True) -> list[tuple]:
        meta = self.get_table_metadata(target_table)
        dtype_date = []
        
        for i, col in enumerate(meta["column_order"]):
            if meta["columns"][col]["data_type"] == "DATE":
                dtype_date.append(i)
        
        
        cursor = self.tables[target_table].cursor()  # 커서 생성
        tmp = []
        while x := cursor.next():
            key, val = x
            key = key.decode()
            val = json.loads(val.decode())
            for i in dtype_date:
                val[i] = datetime.strptime(val[i], '%Y-%m-%d').date()
            if flag:
                tmp.append((key, val))
            else:
                tmp.append(val)
        cursor.close()  # 커서 닫기
        return tmp
        
        
    def table_exist(self, table_name):
        if table_name in self.tables:
            return True
        else:
            return False
        
    def get_table_list(self):
        return [[i] for i in self.tables.keys()]