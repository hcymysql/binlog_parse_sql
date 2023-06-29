#!/usr/bin/env python3
# 从MySQL8.0实时解析binlog并复制到ClickHouse，适用于将MySQL8.0迁移至ClickHouse（ETL抽数据工具）
# 支持DDL和DML语句操作

import os
import pymysql
import signal
import atexit
import re
import time
from queue import Queue
from threading import Thread
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.event import GtidEvent
from clickhouse_driver import Client
import logging


#################修改以下配置配置信息#################
# 源 MySQL 8.0 数据库设置
source_mysql_settings = {
    "host": "192.168.198.239",
    "port": 3336,
    "user": "admin",
    "passwd": "hechunyang",
    "database": "hcy",
    "charset": "utf8mb4",
}

#设置源MySQL Server-Id
source_server_id = 413336

#设置从源同步的binlog文件名和位置点，默认值为 mysql-bin.000001 和 4
binlog_file = "mysql-bin.000123"
binlog_pos = 193

# 目标 ClickHouse 数据库设置
target_clickhouse_settings = {
    "host": "192.168.176.204", # 修改为目标ClickHouse的IP地址或域名
    "port": 9000, # 修改为目标ClickHouse的端口号
    "user": "hechunyang", # 修改为目标ClickHouse的用户名
    "password": "123456", # 修改为目标ClickHouse的密码
    "database": "hcy", # 修改为目标ClickHouse的数据库名
}

LOG_FILE = "ck_repl_status.log"

# 配置日志记录
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

#################以下代码不用修改#################
def convert_mysql_to_clickhouse(mysql_sql):
    # 定义 MySQL 和 ClickHouse 数据类型的映射关系
    type_mapping = {
        'bit': 'UInt8',
        'tinyint': 'Int8',
        'smallint': 'Int16',
        'int': 'Int32',
        'bigint': 'Int64',
        'float': 'Float32',
        'double': 'Float64',
        'decimal': 'Decimal',
        'char': 'String',
        'varchar': 'String',
        'text': 'String',
        'mediumtext': 'String',
        'longtext': 'String',
        'enum': 'String',
        'set': 'String',
        'blob': 'String',
        'varbinary': 'String',
        'time': 'FixedString(8)',
        'datetime': 'DateTime',
        'timestamp': 'DateTime',
        'date': 'DateTime'
        # 添加更多的映射关系...
    }

    # 对于每个映射关系进行替换
    clickhouse_sql = mysql_sql
    for mysql_type, clickhouse_type in type_mapping.items():
        clickhouse_sql = re.sub(r'\b{}\b'.format(mysql_type), clickhouse_type, clickhouse_sql, flags=re.IGNORECASE)

    clickhouse_sql = re.sub(r'\badd\b', 'ADD COLUMN', clickhouse_sql, flags=re.IGNORECASE)
    clickhouse_sql = re.sub(r'\bdrop\b', 'DROP COLUMN', clickhouse_sql, flags=re.IGNORECASE)
    clickhouse_sql = re.sub(r'\bmodify\b', 'MODIFY COLUMN', clickhouse_sql, flags=re.IGNORECASE)
    # 使用正则表达式匹配 CHANGE 语句
    match = re.search(r'ALTER TABLE\s+(\w+)\s+CHANGE\s+(\w+)\s+(\w+)\s+(\w+)', clickhouse_sql, flags=re.IGNORECASE)
    if match:
        table_name = match.group(1)
        old_column_name = match.group(2)
        new_column_name = match.group(3)
        data_type = match.group(4)

        # 判断字段名是否相同
        if old_column_name == new_column_name:
            clickhouse_sql = f'ALTER TABLE {table_name} MODIFY COLUMN {old_column_name} {data_type}'
        else:
            clickhouse_sql = f'ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name}; '
            clickhouse_sql += f'ALTER TABLE {table_name} MODIFY COLUMN {new_column_name} {data_type}'
    
    return clickhouse_sql

# 保存 binlog 位置和文件名
def save_binlog_pos(binlog_file, binlog_pos):
    current_binlog_file = binlog_file
    if binlog_file is not None:
        with open('binlog_info.txt', 'w') as f:
            f.write('{}\n{}'.format(current_binlog_file, binlog_pos))
        print('Binlog position ({}, {}) saved.'.format(current_binlog_file, binlog_pos))
    else:
        with open('binlog_info.txt', 'w') as f:
            f.write('{}\n{}'.format(current_binlog_file, binlog_pos))
        print('Binlog position ({}, {}) updated.'.format(current_binlog_file, binlog_pos))

# 读取上次保存的 binlog 位置和文件名
def load_binlog_pos():
    global binlog_file, binlog_pos
    try:
        with open('binlog_info.txt', 'r') as f:
            binlog_file, binlog_pos = f.read().strip().split('\n')
    except FileNotFoundError:
        binlog_file, binlog_pos = binlog_file, binlog_pos
    except Exception as e:
        print('Load binlog position failure:', e)
        binlog_file, binlog_pos = binlog_file, binlog_pos

    return binlog_file, int(binlog_pos)


# 退出程序时保存当前的 binlog 文件名和位置点
def exit_handler(stream, current_binlog_file, binlog_pos):
    stream.close()
    #save_binlog_pos(current_binlog_file, binlog_pos)
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)

# 在程序被终止时保存当前的 binlog 文件名和位置点
def save_binlog_pos_on_termination(signum, frame):
    save_binlog_pos(current_binlog_file, binlog_pos or stream.log_pos)
    quit_program()

# 退出程序时保存当前的 binlog 文件名和位置点
def quit_program():
    stream.close()
    target_conn.close()
    exit(0)

# 建立连接
target_conn = Client(**target_clickhouse_settings)

saved_pos = load_binlog_pos()

# 定义队列用于存放 SQL 语句并作为解析 binlog 和执行 SQL 语句的中间件
sql_queue = Queue()

# 定义 SQL 执行函数，从队列中取出 SQL 语句并依次执行
def sql_worker():
    while True:
        sql = sql_queue.get()
        try:
            # https://blog.csdn.net/mdh17322249/article/details/123966953
            for sql_s in sql.split(';'):
                single_sql = sql_s.strip()
                if single_sql:
                    target_conn.execute(single_sql)
            print(f"Success to execute SQL: {sql}")
            current_timestamp = int(time.time())
            Seconds_Behind_Master = current_timestamp - event_time
            print(f"入库延迟时间为：{Seconds_Behind_Master} （单位秒）")
            logging.info(f"入库延迟时间为：{Seconds_Behind_Master} （单位秒）")
        except Exception as e:
            logging.error(f"Failed to execute SQL: {sql}")
            logging.error(f"Error message: {e}")
        finally:
            sql_queue.task_done()

# 启动 SQL 执行线程
sql_thread = Thread(target=sql_worker, daemon=True)
sql_thread.start()

# https://python-mysql-replication.readthedocs.io/en/latest/_modules/pymysqlreplication/binlogstream.html#BinLogStreamReader
stream = BinLogStreamReader(
    connection_settings=source_mysql_settings,
    server_id=source_server_id,  
    blocking=True,
    resume_stream=True,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
    log_file=saved_pos[0],
    log_pos=saved_pos[1]
)

# 循环遍历解析出来的行事件并存入SQL语句中
def process_rows_event(binlogevent, stream):
    if hasattr(binlogevent, "schema"):
        database_name = binlogevent.schema  # 获取数据库名
    else:
        database_name = None

    global event_time
    event_time = binlogevent.timestamp

    if isinstance(binlogevent, QueryEvent):
        mysql_sql = binlogevent.query
        print(mysql_sql)

        clickhouse_sql = convert_mysql_to_clickhouse(mysql_sql)
        print(clickhouse_sql)

        sql_queue.put(clickhouse_sql) # 将 SQL 语句加入队列
    else:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                values = list(row["values"].values())
                set_values = []
                for value in values:
                    if value is None:
                        set_values.append('NULL')
                    elif isinstance(value, str):
                        set_values.append(f"'{value}'")
                    else:
                        set_values.append(str(value))
                sql = "INSERT INTO {}({}) VALUES ({})".format(
                    f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                    '`' + '`,`'.join(list(row["values"].keys())) + '`',
                    ','.join(set_values)
                )
                """
                sql = "INSERT INTO {}({}) VALUES ({})".format(
                    f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                    '`' + '`,`'.join(list(row["values"].keys())) + '`',
                    ','.join(["'%s'" % str(i) for i in list(row["values"].values())])
                )
                """
                print(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列
    
            elif isinstance(binlogevent, UpdateRowsEvent):
                db = pymysql.connect(**source_mysql_settings)
                cursor = db.cursor()
                database_name = source_mysql_settings["database"]
                query = f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='{database_name}' 
                        AND COLUMN_KEY='PRI'
                """
                cursor.execute(query)
                primary_keys = [row[0] for row in cursor.fetchall()]

                set_values = []
                for k, v in row["after_values"].items():
                    if k not in primary_keys:
                        if v is None:
                            set_values.append(f"`{k}`=NULL")
                        elif isinstance(v, str):
                            set_values.append(f"`{k}`='{v}'")
                        else:
                            set_values.append(f"`{k}`={v}")
                set_clause = ','.join(set_values)

                where_values = []
                for k, v in row["before_values"].items():
                    if isinstance(v, str):
                        where_values.append(f"`{k}`='{v}'")
                    else:
                        where_values.append(f"`{k}`={v}")
                where_clause = ' AND '.join(where_values)

                # https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse
                sql = f"ALTER TABLE `{binlogevent.table}` UPDATE {set_clause} WHERE {where_clause}"
                print(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列
    
            elif isinstance(binlogevent, DeleteRowsEvent):
                sql = "ALTER TABLE `{}` DELETE WHERE {}".format(
                    f"{binlogevent.table}" if database_name else binlogevent.table,
                    ' AND '.join(["`{}`='{}'".format(k, v) for k, v in row["values"].items()])
                )
                print(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列
                    
    return binlogevent.packet.log_pos


# 循环遍历解析出来的行事件并存入SQL语句中
while True:
    try:
        for binlogevent in stream:
            #print(f'binlog: {stream.log_file}, positon: {stream.log_pos}')
            current_binlog_file = stream.log_file
            try:
                binlog_pos = process_rows_event(binlogevent, stream)
                #print(f'binlog_pos :=====> {binlog_pos}')
            except AttributeError as e:
                save_binlog_pos(current_binlog_file, binlog_pos)
            else:
                save_binlog_pos(current_binlog_file, binlog_pos)

    except KeyboardInterrupt:
        save_binlog_pos(current_binlog_file, binlog_pos)
        break

    except pymysql.err.OperationalError as e:
        print("MySQL Error {}: {}".format(e.args[0], e.args[1]))

# 等待所有 SQL 语句执行完毕
sql_queue.join()

# 在程序退出时保存 binlog 位置
atexit.register(exit_handler, stream, current_binlog_file, binlog_pos)

# 接收 SIGTERM 和 SIGINT 信号
signal.signal(signal.SIGTERM, save_binlog_pos_on_termination)
signal.signal(signal.SIGINT, save_binlog_pos_on_termination)

# 关闭连接
atexit.register(target_conn.close)
