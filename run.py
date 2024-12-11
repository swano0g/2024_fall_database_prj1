import os
from lark import Lark, UnexpectedInput, Transformer
import src.MyTransformer as MyTransformer
import src.DatabaseHandler as DatabaseHandler
from berkeleydb import db



id = "2023-11225"
env_path="DB"
db_file="my_database.db"

if not os.path.exists(env_path):
    os.makedirs(env_path)

# 데이터베이스를 열고 이를 DatabaseHandler 객체에 넘겨준다.
database_env = db.DBEnv()
database_env.open(env_path, db.DB_CREATE | db.DB_INIT_MPOOL)
db_handler = DatabaseHandler.DatabaseHandler(database_env)

with open('grammar.lark', 'r') as file:
    sql_grammar = file.read()


# sql 문법을 파싱할 파서 생성. 
sql_parser = Lark(sql_grammar, start='command',  lexer='basic')

# 파싱된 sql 명령을 입력받아 명령을 수행하는 객체. 위에서 만든 데이터베이스 핸들러 객체를 입력으로 받는다.
myTransformer = MyTransformer.MyTransformer(id, db_handler)

def prompt():
    """
    This function receives user input, splits the command based on semicolons, and returns.
    """
    
    prompt_input = ""
    while True:
        t_input = input(f"DB_{id}> ")
        prompt_input += t_input + " "
        if len(t_input) == 0:
            pass
        elif t_input[-1] == ";":
            break
    
    query_list = []
    s = 0
    for i, j in enumerate(prompt_input):
        if j == ";":
            query_list.append(prompt_input[s:i+1].strip())
            s = i + 1
    return query_list


def main():
    while True: 
        query = prompt()
        
        try:
            for command in query:
                output = sql_parser.parse(command)
                myTransformer.transform(output)
        
        # sql 문법에 오류가 있는 경우 Syntax error 메세지를 띄운다.
        except UnexpectedInput:
            print(f"DB_{id}> Syntax error")
        
        # 명령어의 내용에 오류가 있는 경우 그에 따른 에러 메세지를 띄운다.
        except Exception as e:
            print(f'DB_{id}> {e.orig_exc}')

if __name__ == '__main__':
    main()
