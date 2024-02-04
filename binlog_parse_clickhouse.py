#!/usr/bin/env python3
"""
- 从MySQL8.0实时解析binlog并复制到ClickHouse，适用于将MySQL8.0迁移至ClickHouse（ETL抽数据工具）
- 支持DDL和DML语句操作（注：解析binlog转换（ClickHouse）CREATE TABLE语法较复杂，暂不支持）
- 支持只同步某几张表或忽略同步某几张表
- python3.10编译安装报SSL失败解决方法
- https://blog.csdn.net/mdh17322249/article/details/123966953
"""

import os,sys
import pymysql
import signal
import atexit
import re
import time,datetime
import json
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
import yaml
import argparse

# 创建命令行参数解析器
parser = argparse.ArgumentParser()
# 添加-f/--file参数，用于指定db.yaml文件的路径
parser.add_argument("-f", "--file", required=True, help="Path to db.yaml file")

# 解析命令行参数
args = parser.parse_args()

# 获取传入的db.yaml文件路径
file_path = args.file

# 读取YAML配置文件
with open(file_path, 'r') as file:
    config = yaml.safe_load(file)

# 将YAML配置信息转换为变量
source_mysql_settings = config.get('source_mysql_settings', {})
source_server_id = config.get('source_server_id', None)
binlog_file = config.get('binlog_file', '')
binlog_pos = config.get('binlog_pos', 4)
ignore_tables = config.get('ignore_tables', None)  # 默认值修改为None
ignore_prefixes = config.get('ignore_prefixes', None)
repl_tables = config.get('repl_tables', None)
repl_prefixes = config.get('repl_prefixes', None)
target_clickhouse_settings = config.get('target_clickhouse_settings', {})
clickhouse_cluster_name = config.get('clickhouse_cluster_name', None)
LOG_FILE = config.get('LOG_FILE', '')

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

    """
    解析binlog转换（ClickHouse）CREATE TABLE语法较复杂，想办法解决。
    <code>代码块……</code>
    """

    if 'clickhouse_cluster_name' in globals():
        clickhouse_sql = re.sub(r'\badd\b', f' ON CLUSTER {clickhouse_cluster_name} ADD COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\badd\s+column\b', f' ON CLUSTER {clickhouse_cluster_name} ADD COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bdrop\b', f' ON CLUSTER {clickhouse_cluster_name} DROP COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bdrop\s+column\b', f' ON CLUSTER {clickhouse_cluster_name} DROP COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bmodify\b', f' ON CLUSTER {clickhouse_cluster_name} MODIFY COLUMN', clickhouse_sql, flags=re.IGNORECASE)
    else:
        clickhouse_sql = re.sub(r'\badd\b', 'ADD COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\badd\s+column\b', 'ADD COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bdrop\b', 'DROP COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bdrop\s+column\b', 'DROP COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        clickhouse_sql = re.sub(r'\bmodify\b', 'MODIFY COLUMN', clickhouse_sql, flags=re.IGNORECASE)

    # 当rename table t1 to t2不被转换；当alter table t1 rename cid to cid2才会被转换
    if re.search(r'(?<!\S)rename(?!\S)', clickhouse_sql,flags=re.IGNORECASE) and not clickhouse_sql.strip().lower().startswith("rename"):
        if 'clickhouse_cluster_name' in globals():
            clickhouse_sql = re.sub(r'(?<!\S)rename(?!\S)column', f' ON CLUSTER {clickhouse_cluster_name} RENAME COLUMN', clickhouse_sql, flags=re.IGNORECASE)
        else:
            clickhouse_sql = re.sub(r'(?<!\S)rename(?!\S)column', 'RENAME COLUMN', clickhouse_sql, flags=re.IGNORECASE)
    elif clickhouse_sql.strip().lower().startswith("rename"):
        if 'clickhouse_cluster_name' in globals():
            clickhouse_sql += f' ON CLUSTER {clickhouse_cluster_name}'

    # 使用正则表达式匹配 CHANGE 语句
    match = re.search(r'ALTER TABLE\s+(\w+)\s+CHANGE\s+(\w+)\s+(\w+)\s+(\w+)', clickhouse_sql, flags=re.IGNORECASE)
    if match:
        table_name = match.group(1)
        old_column_name = match.group(2)
        new_column_name = match.group(3)
        data_type = match.group(4)

        # 判断字段名是否相同
        if old_column_name == new_column_name:
            if 'clickhouse_cluster_name' in globals():
                clickhouse_sql = f'ALTER TABLE {table_name} ON CLUSTER {clickhouse_cluster_name} MODIFY COLUMN {old_column_name} {data_type}'
            else:
                clickhouse_sql = f'ALTER TABLE {table_name} MODIFY COLUMN {old_column_name} {data_type}'
        else:
            if 'clickhouse_cluster_name' in globals():
                clickhouse_sql = f'ALTER TABLE {table_name} ON CLUSTER {clickhouse_cluster_name} RENAME COLUMN {old_column_name} TO {new_column_name}; '
                clickhouse_sql += f'ALTER TABLE {table_name} ON CLUSTER {clickhouse_cluster_name} MODIFY COLUMN {new_column_name} {data_type}'
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
    # save_binlog_pos(current_binlog_file, binlog_pos)
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
            if sql.strip().upper() == 'BEGIN' or sql.strip().upper() == 'COMMIT':
                # 跳过事务语句
                continue

            # 执行其他SQL语句
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
            os.kill(os.getpid(), signal.SIGTERM)
        finally:
            sql_queue.task_done()


# 启动 SQL 执行线程
sql_thread = Thread(target=sql_worker, daemon=True)
#sql_thread = Thread(target=sql_worker)
sql_thread.start()

####################################################################################################################
def ignore_table_filter(table_name):
    global ignore_tables, ignore_prefixes

    if 'ignore_tables' in globals() and ignore_tables is not None and table_name in ignore_tables:
        return True

    if 'ignore_prefixes' in globals() and ignore_prefixes is not None:
        for prefix in ignore_prefixes:
            if re.match(prefix, table_name):
                return True

    return False

def repl_table_filter(table_name):
    global repl_tables, repl_prefixes

    if 'repl_tables' in globals() and repl_tables is not None and table_name in repl_tables:
        return True

    if 'repl_prefixes' in globals() and repl_prefixes is not None:
        for prefix in repl_prefixes:
            if re.match(prefix, table_name):
                return True

    return False

# 要排除的数据库列表
excluded_databases = ['mysql', 'sys', 'information_schema', 'performance_schema', 'test']

if 'repl_tables' in globals() and repl_tables is not None:
    all_tables = repl_tables
elif 'ignore_tables' in globals() and ignore_tables is not None:
    all_tables = ignore_tables
else:
    all_tables = []

# 如果设置了 ignore_prefixes 或 repl_prefixes，则获取所有表信息
if ('ignore_prefixes' in globals() and ignore_prefixes is not None) or ('repl_prefixes' in globals() and repl_prefixes is not None):
    # 连接MySQL数据库
    connection = pymysql.connect(**source_mysql_settings)
    cursor = connection.cursor()

    # 获取所有数据库名
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    database_names = [database[0] for database in databases]

    # 过滤要排除的数据库
    filtered_database_names = [database for database in database_names if database not in excluded_databases]

    # 获取每个数据库的所有表名
    for database_name in filtered_database_names:
        cursor.execute(f"USE `{database_name}`")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        all_tables.extend(table_names)

    # 关闭数据库连接
    cursor.close()
    connection.close()
    
# https://python-mysql-replication.readthedocs.io/en/latest/_modules/pymysqlreplication/binlogstream.html#BinLogStreamReader
stream = BinLogStreamReader(
    connection_settings=source_mysql_settings,
    server_id=source_server_id,
    blocking=True,
    resume_stream=True,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent],
    log_file=saved_pos[0],
    log_pos=saved_pos[1],
    only_tables = [table_name for table_name in all_tables if repl_table_filter(table_name)] \
        if ('repl_tables' in globals() and repl_tables is not None) or ('repl_prefixes' in globals() and repl_prefixes is not None) else None,
    ignored_tables = [table_name for table_name in all_tables if ignore_table_filter(table_name)] \
        if ('ignore_tables' in globals() and ignore_tables is not None) or ('ignore_prefixes' in globals() and ignore_prefixes is not None) else None
)

# 循环遍历解析出来的行事件并存入SQL语句中
def process_rows_event(binlogevent, stream):

    def convert_bytes_to_str(data):
        if isinstance(data, dict):
            return {convert_bytes_to_str(key): convert_bytes_to_str(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [convert_bytes_to_str(item) for item in data]
        elif isinstance(data, bytes):
            return data.decode('utf-8')
        else:
            return data

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

        sql_queue.put(clickhouse_sql)  # 将 SQL 语句加入队列
    else:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                values = list(row["values"].values())
                set_values = []
                for value in values:
                    if value is None:
                        set_values.append('NULL')
                    elif isinstance(value, (str, datetime.datetime, datetime.date)):
                        set_values.append(f"'{value}'")
                    elif isinstance(value, str):
                        set_values.append(f"'{value}'")
                    elif isinstance(value, dict):
                        v = json.dumps(convert_bytes_to_str(value))
                        set_values.append(f"'{v}'")
                    else:
                        set_values.append(str(value))
                        
                sql = "INSERT INTO {}({}) VALUES ({})".format(
                    f"`{database_name}`.`{binlogevent.table}`" if database_name else binlogevent.table,
                    '`' + '`,`'.join(list(row["values"].keys())) + '`',
                    ','.join(set_values)
                )

                #print(sql)
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
                        elif isinstance(v, (str, datetime.datetime, datetime.date)):
                            set_values.append(f"`{k}`='{v}'")
                        elif isinstance(v, dict):
                            v=json.dumps(convert_bytes_to_str(v))
                            set_values.append(f"`{k}`='{v}'")
                        else:
                            set_values.append(f"`{k}`={v}")
                set_clause = ','.join(set_values)

                where_values = []
                for k, v in row["before_values"].items():
                    if isinstance(v, (str, datetime.datetime, datetime.date)):
                        where_values.append(f"`{k}`='{v}'")
                    elif isinstance(v, dict):
                        v=json.dumps(convert_bytes_to_str(v))
                        where_values.append(f"`{k}`='{v}'")
                    else:
                        where_values.append(f"`{k}`={v}")
                where_clause = ' AND '.join(where_values)

                # https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse
                sql = f"ALTER TABLE `{binlogevent.table}` UPDATE {set_clause} WHERE {where_clause}"
                #print(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列

            elif isinstance(binlogevent, DeleteRowsEvent):
                sql = "ALTER TABLE {} DELETE WHERE {};".format(
                    "`{}`.`{}`".format(database_name, binlogevent.table) if database_name else "`{}`".format(
                        binlogevent.table),
                    ' AND '.join(["`{}`={}".format(k,"'{}'".format(v) if isinstance(v, (
                    str, datetime.datetime, datetime.date))
                    else 'NULL' if v is None
                    else "'{}'".format(json.dumps(convert_bytes_to_str(v))) if isinstance(v, (
                    dict)) else str(v))
                    for k, v in row["values"].items()])
                )
                #print(sql)
                sql_queue.put(sql)  # 将 SQL 语句加入队列

    return binlogevent.packet.log_pos


# 循环遍历解析出来的行事件并存入SQL语句中
while True:
    try:
        for binlogevent in stream:
            # print(f'binlog: {stream.log_file}, positon: {stream.log_pos}')
            current_binlog_file = stream.log_file
            try:
                binlog_pos = process_rows_event(binlogevent, stream)
                # print(f'binlog_pos :=====> {binlog_pos}')
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
