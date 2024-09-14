import mysql.connector
from mysql.connector import Error
from tabulate import tabulate
import yaml

"""
config = {
    host: localhost
    user: root
    password: your password
    database: try "ddl_manager"
}
"""
def load_config(filename="config.yaml"):
    with open(filename, 'r') as file:
        return yaml.safe_load(file)

config = load_config()

# 全局变量来存储数据库连接和游标
conn = None
cursor = None


def create_database(db_config, database_name):
    try:
        conn = mysql.connector.connect(**db_config)
        print("Connected to MySQL server")
        cursor = conn.cursor()

        cursor.execute("SHOW DATABASES")
        db_exists = database_name in [db[0] for db in cursor.fetchall()]

        if not db_exists:
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Database '{database_name}' created")
        else:
            print(f"Database '{database_name}' already exists")

        cursor.close()
        conn.close()

    except Error as e:
        print("Error while connecting to MySQL server", e)
    finally:
        if conn:
            conn.close()


# 创建数据库
create_database({
    'host': config['host'],
    'user': config['user'],
    'password': config['password']
}, config['database'])

# 连接到 MySQL 数据库
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print(f"Successfully connected to database : {config['database']}")
except Error as e:
    print("Error000: Error while connecting to MySQL", e)
    conn = None

# 创建DDL表
ddl_table_query = """
CREATE TABLE IF NOT EXISTS ddl1 (
    abs_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    yields VARCHAR(255) NOT NULL,
    deadline DATE NOT NULL,
    importance INT NOT NULL,
    difficulty INT NOT NULL,
    estimate INT NOT NULL
);
"""

yields_table_query = """
CREATE TABLE IF NOT EXISTS YIELDS1 (
    abs_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
"""

if conn:
    try:
        cursor.execute(ddl_table_query)
        cursor.execute(yields_table_query)
        print("DDL and YIELDS tables exist or have been created successfully.")
    except Error as e:
        print("Error007: Failed to create table", e)

def print_help():
    print("Available commands:")
    print("  ADD YIELD [name]")
    print("  ADD DDL [name] [yields] [deadline] [importance] [difficulty] [estimate]")
    print("  QUERY YIELD")
    print("  QUERY DDLS")
    print("  QUERY ALL")
    print("  DELETE YIELD [name]")
    print("  DELETE DDL [id]")
    print(" DDL [characteristic] [DESC/ASC]")

def run():
    command = input()
    kw = command.split()

    if kw[0] == "HELP":
        print_help()

    if len(kw) == 1:
        print("Error001: You have an error in your command syntax; Try to insert again or use 'HELP' for assistance.")

    if len(kw) == 2:
        fw,sw = kw

        if fw == "ADJUST" and (sw == "DDL" or sw == "DDLS"):
            print("QUERY Ok, Now adjusting:[id] [new_deadline]")
            ans = input()
            id = ans.split()[0]
            nd = ans.split()[1]
            sql_com = "UPDATE ddl1 SET deadline = %s WHERE abs_id = %s"
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute(sql_com,(nd,id))
                conn.commit()
                print("ADJUST OK")
            except Error as e:
                print("Error101: Failed to Adjust ddl: ", e)

        if fw == "ADD" and (sw == "YIELD" or sw == "YIELDS") :
            print("QUERY Ok, Now adding:[name]")
            name = input()
            sql_com = "INSERT INTO YIELDS1(name) VALUE(%s)"
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute(sql_com, (name,))
                conn.commit()
                print("YIELD added successfully")
            except Error as e:
                print("Error101: Failed to insert YIELD: ", e)

        if fw == "ADD" and (sw == "DDL" or sw == "DDLS"):
            print("QUERY Ok, Now adding:[name] [yields] [deadline] [importance] [difficulty] [estimate]")
            data = input().split()
            name,yields,deadline,importance,difficulty,estimate = data
            sql_com = "INSERT INTO ddl1(name,yields,deadline,importance,difficulty,estimate) VALUES(%s,%s,%s,%s,%s,%s)"
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute(sql_com, (name, yields, deadline, importance,difficulty,estimate))
                conn.commit()
                print("DDL added successfully")
            except Error as e:
                print("Error102: Failed to insert DDL: ", e)

        if fw == "QUERY":
            if fw == "QUERY" and (sw == "YIELD" or sw == "YIELDS"):
                sql_com = "SELECT * FROM YIELDS1"
            if fw == "QUERY" and (sw == "DDL" or sw == "DDLS"):
                sql_com = "SELECT * FROM ddl1"
            if fw == "QUERY" and sw == "ALL":
                sql_com = "SELECT * FROM ddl1 LEFT JOIN YIELDS1 ON ddl1.yields = YIELDS1.name"
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute(sql_com)
                rows = cursor.fetchall()
                # 获取列名
                column_names = [i[0] for i in cursor.description]
                # 使用tabulate来格式化输出
                print(tabulate(rows, headers=column_names, tablefmt="grid"))
            except Error as e:
                print("Error201 : Failed to query DDL: ", e)
            finally:
                if cursor:
                    cursor.close()

        if fw == "DELETE":
            if sw == "YIELD" or sw == "YIELDS":
                print("DELETE REQUEST Ok, input name")
                para = input()
                sql_com = "DELETE FROM YIELDS1 WHERE name = %s"
                try:
                    cursor = conn.cursor(buffered=True)
                    cursor.execute(sql_com, (para,))
                    conn.commit()
                    print("deleted successfully")
                except Error as e:
                    print("Error 202: Failed to delete: ", e)
            if sw == "DDL" or sw == "DDLS":
                print("DELETE REQUEST Ok, input name")
                para = input()
                sql_com = "DELETE FROM ddl1 WHERE name = %s"
                try:
                    cursor = conn.cursor(buffered=True)
                    cursor.execute(sql_com, (para,))
                    conn.commit()
                    print("deleted successfully")
                except Error as e:
                    print("Error 202: Failed to delete: ", e)

    if len(kw) == 3:
        fw,mw,lw = kw
        if fw != "SHOW":
            print("Error001: You have an error in your command syntax; Try to insert again or use 'HELP' for assistance.")
        else:
            if mw == "DEADLINE" and lw == "ASC":
                sql_com = "SELECT * FROM ddl1 ORDER BY deadline ASC"
            if mw == "DEADLINE" and lw == "DESC":
                sql_com = "SELECT * FROM ddl1 ORDER BY deadline DESC"
            if mw == "IMPORTANCE" and lw == "DESC":
                sql_com = "SELECT * FROM ddl1 ORDER BY importance DESC"
            if mw == "IMPORTANCE" and lw == "ASC":
                sql_com = "SELECT * FROM ddl1 ORDER BY importance ASC"
            if mw == "DIFFICULTY" and lw == "DESC":
                sql_com = "SELECT * FROM ddl1 ORDER BY difficulty DESC"
            if mw == "DIFFICULTY" and lw == "ASC":
                sql_com = "SELECT * FROM ddl1 ORDER BY importance ASC"
            if mw == "TIME" and lw == "DESC":
                sql_com = "SELECT * FROM ddl1 ORDER BY estimate DESC"
            if mw == "TIME" and lw == "ASC":
                sql_com = "SELECT * FROM ddl1 ORDER BY estimate ASC"
            if mw == "GROUP" and lw == "DESC":
                sql_com = "SELECT yields AS yield, SUM(estimate) AS total_time FROM ddl1 GROUP BY yield ORDER BY total_time DESC"
            if mw == "GROUP" and lw == "ASC":
                sql_com = "SELECT yields AS yield, SUM(estimate) AS total_time FROM ddl1 GROUP BY yield ORDER BY total_time ASC"
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute(sql_com)
                rows = cursor.fetchall()
                column_names = [i[0] for i in cursor.description]
                # 使用tabulate来格式化输出
                print(tabulate(rows, headers=column_names, tablefmt="grid"))
            except Error as e:
                print("Error301 : Failed to query DDL: ", e)
            finally:
                if cursor:
                    cursor.close()

    if len(kw) >=4:
        print("Error001: You have an error in your command syntax; Try to insert again or use 'HELP' for assistance.")

def main():
    while True:
        run()

if __name__ == "__main__":
    main()

# 确保在程序退出时关闭数据库连接
if conn:
    conn.close()