#!/usr/bin/env python3
# MySQL表结构转换为ClickHouse表结构，仅为单库。

import pymysql
import re
from clickhouse_driver import Client
import logging

#################修改以下配置配置信息#################
# 配置信息
MYSQL_HOST = "192.168.198.239"
MYSQL_PORT = 3336
MYSQL_USER = "admin"
MYSQL_PASSWORD = "hechunyang"
MYSQL_DATABASE = "hcy"

CLICKHOUSE_HOST = "192.168.176.204"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "hechunyang"
CLICKHOUSE_PASSWORD = "123456"
CLICKHOUSE_DATABASE = "hcy"

LOG_FILE = "convert_error.log"

# 配置日志记录
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

#################以下代码不用修改#################
def convert_field_type(field_type):
    """
    将MySQL字段类型转换为ClickHouse字段类型
    """
    field_type = field_type.split()[0]
    if "tinyint" in field_type:
        return "Int8"
    elif "smallint" in field_type:
        return "Int16"
    elif "mediumint" in field_type:
        return "Int32"
    elif field_type.startswith("int"):
        return "Int32"
    elif field_type.startswith("bigint"):
        return "Int64"
    elif "float" in field_type:
        return "Float32"
    elif "double" in field_type or "numeric" in field_type:
        return "Float64"
    elif "decimal" in field_type:
        return "Decimal128(2)"
    elif "datetime" in field_type or "timestamp" in field_type or "date" in field_type:
        return "DateTime"
    elif "char" in field_type or "varchar" in field_type or "text" in field_type or "enum" in field_type or "set" in field_type:
        return "String"
    elif "bit" in field_type:
        return "UInt8"
    elif "time" in field_type:
        return "FixedString(8)"
    elif "blob" in field_type:
        return "String"
    elif "varbinary" in field_type:
        return "String"
    elif field_type.startswith("bit"):
        return "UInt8"
    else:
        raise ValueError(f"无法转化未知 MySQL 字段类型：{field_type}")

def convert_mysql_to_clickhouse(mysql_conn, mysql_database, table_name, clickhouse_conn, clickhouse_database):
    """
    将MySQL表结构转换为ClickHouse
    """
    # 获取MySQL表结构
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"SHOW KEYS FROM {mysql_database}.{table_name} WHERE Key_name = 'PRIMARY'")
    mysql_primary_key = [key[4] for key in mysql_cursor.fetchall()]

    mysql_cursor.execute(f"DESCRIBE {mysql_database}.{table_name}")
    mysql_columns = mysql_cursor.fetchall()

    # 创建ClickHouse表
    clickhouse_columns = []
    create_statement = "CREATE TABLE IF NOT EXISTS " + clickhouse_database + "." + table_name + " ("
    for mysql_column in mysql_columns:
        column_name = mysql_column[0]
        column_type = mysql_column[1]

        # 转换字段类型
        clickhouse_type = convert_field_type(column_type)

        # 拼接SQL语句
        create_statement += f"{column_name} {clickhouse_type},"

        # 添加到columns列表中
        clickhouse_columns.append(column_name)

    # 设置主键
    primary_key_str = ",".join(mysql_primary_key)
    create_statement += f"PRIMARY KEY ({primary_key_str})"

    # 设置存储引擎为 MergeTree
    create_statement += ") ENGINE = MergeTree ORDER BY " + ','.join(mysql_primary_key)

    # 执行SQL语句
    try:
        clickhouse_cursor = clickhouse_conn.execute(create_statement)
    except Exception as e:
        logging.error(f"执行SQL语句失败：{create_statement}")
        #logging.error(f"错误信息：{e}")

    # 输出ClickHouse表结构
    # print(f"ClickHouse create statement: {create_statement}")

def convert_mysql_database_to_clickhouse(mysql_conn, mysql_database, clickhouse_conn, clickhouse_database):
    """
    将MySQL数据库表结构转换为ClickHouse
    """
    # 获取MySQL库中所有表
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"SHOW TABLES FROM {mysql_database}")
    tables = mysql_cursor.fetchall()

    # 遍历所有表进行转换
    for table in tables:
        table_name = table[0]
        convert_mysql_to_clickhouse(mysql_conn, mysql_database, table_name, clickhouse_conn, clickhouse_database)

if __name__ == "__main__":
    # 连接MySQL数据库
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    # 连接ClickHouse数据库
    clickhouse_conn = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DATABASE)

    # 转化表结构
    convert_mysql_database_to_clickhouse(mysql_conn, MYSQL_DATABASE, clickhouse_conn, CLICKHOUSE_DATABASE)
