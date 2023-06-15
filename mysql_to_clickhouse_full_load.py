#!/usr/bin/env python3
# MySQL全量数据导入到ClickHouse里，默认并行10张表同时导出数据，每次轮询取1000条数据。
# 使用条件：表必须有自增主键，测试环境MySQL 8.0

import pymysql.cursors
from clickhouse_driver import Client
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import datetime

mysql_config = {
    'host': '192.168.198.239',
    'port': 3336,
    'user': 'admin',
    'password': 'hechunyang',
    'db': 'hcy',
    'charset': 'utf8mb4'
}

clickhouse_config = {
    'host': '192.168.176.204',
    'port': 9000,
    'user': 'hechunyang',
    'password': '123456',
    'database': 'hcy'
}

mysql_connection = pymysql.connect(**mysql_config, autocommit=False, cursorclass=pymysql.cursors.DictCursor)
mysql_connection.begin()
try:
    with mysql_connection.cursor() as cursor:
        cursor.execute("SET transaction_isolation = 'REPEATABLE-READ'");
        cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT") # 设置一致性快照
        cursor.execute("SHOW TABLES")
        result = cursor.fetchall()
        tables = [val for d in result for val in d.values()]
        table_bounds = {}
        for table_name in tables:
            cursor.execute("SELECT MIN(_rowid) AS `MIN(id)`, MAX(_rowid) AS `MAX(id)` FROM `{}`".format(table_name))
            row = cursor.fetchone()
            min_id, max_id = row['MIN(id)'], row['MAX(id)']
            table_bounds[table_name] = (min_id, max_id)
        
        cursor.execute("SHOW MASTER STATUS") # 获取当前的binlog文件名和位置点信息
        binlog_row = cursor.fetchone()
        binlog_file, binlog_position, gtid = binlog_row['File'], binlog_row['Position'], binlog_row['Executed_Gtid_Set']
        
        # 将binlog文件名、位置点和GTID信息保存到metadata.txt文件中
        with open('metadata.txt', 'w') as f:
            f.write('{}\n{}\n{}'.format(binlog_file, binlog_position, gtid))
    #mysql_connection.commit()
#finally:
except Exception as e:
    print(e)
    #mysql_connection.close()

def read_from_mysql(table_name, start_id, end_id):
    mysql_connection = pymysql.connect(**mysql_config, cursorclass=pymysql.cursors.DictCursor)
    try:
        with mysql_connection.cursor() as cursor:
            query = "SELECT * FROM {} WHERE _rowid >= {} AND _rowid < {}".format(table_name, start_id, end_id)
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    finally:
        mysql_connection.close()

def insert_into_clickhouse(table_name, records):
    clickhouse_client = Client(**clickhouse_config)
    try:
        column_names = list(records[0].keys())
        values_list = []
        for record in records:
            values = []
            for column_name in column_names:
                value = record[column_name]
                if isinstance(value, str) or isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                    values.append(f"'{value}'")
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    values.append(f"'{str(value)}'")
            values_list.append(f"({','.join(values)})")
        query = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES {','.join(values_list)}"
        clickhouse_client.execute(query)
        ###调试使用
        #print(f"执行的SQL是：{query}") 
    except Exception as e:
        print('Error inserting records into ClickHouse:', e)
    finally:
        clickhouse_client.disconnect()

def worker(table_name):
    min_id, max_id = table_bounds[table_name]
    if min_id == max_id:  # 如果表只有一条记录，则直接处理
        records = read_from_mysql(table_name, min_id, max_id+1)
        print(f"Retrieved {len(records)} record from MySQL table {table_name} with ID {min_id}")
        if len(records) > 0:
            insert_into_clickhouse(table_name, records)
        return

    row_count = max_id - min_id + 1
    if row_count <= 1000:  # 如果行数小于等于 1000，则将批处理大小设置为行数
        batch_size = row_count
    else:
        batch_size = 1000  # 否则，将批处理大小设置为默认值 1000

    with ThreadPoolExecutor(max_workers=10) as executor:
        for start_id in range(min_id, max_id, batch_size):
            end_id = start_id + batch_size
            if end_id > max_id:
                end_id = max_id + 1
            records = read_from_mysql(table_name, start_id, end_id)
            print(f"Retrieved {len(records)} records from MySQL table {table_name} between ID {start_id} and {end_id}")
            if len(records) > 0:
                executor.submit(insert_into_clickhouse, table_name, records)

# 并发十张表同时导入数据
with ThreadPoolExecutor(max_workers=10) as executor:
    for table_name in tables:
        executor.submit(worker, table_name)
