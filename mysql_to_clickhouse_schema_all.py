#!/usr/bin/env python3
# MySQL表结构转换为ClickHouse表结构，将MySQL实例下的所有库，转换到ClickHouse实例的相应库下。
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

# 设置ClickHouse集群的名字，这样方便在所有节点上同时创建表引擎ReplicatedMergeTree
# 通过select * from system.clusters命令查看集群的名字
#clickhouse_cluster_name = "perftest_1shards_3replicas"

# 设置表引擎
TABLE_ENGINE = "MergeTree"
#TABLE_ENGINE = "ReplicatedMergeTree"

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
        precision_scale = re.search(r'\((.*?)\)', field_type).group(1)
        precision, scale = precision_scale.split(',')
        return f"Decimal({precision}, {scale})"
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
    if 'clickhouse_cluster_name' in globals() and TABLE_ENGINE == 'ReplicatedMergeTree':
        create_statement = f"CREATE TABLE IF NOT EXISTS {clickhouse_database}.{table_name} ON CLUSTER {clickhouse_cluster_name} ("
    else:
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

    if TABLE_ENGINE == "MergeTree":
        # 设置存储引擎为 MergeTree
        create_statement += ") ENGINE = MergeTree ORDER BY " + ','.join(mysql_primary_key)
    else:
        # 设置存储引擎为 ReplicatedMergeTree
        create_statement += f") ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table_name}', '{{replica}}') ORDER BY " + ','.join(mysql_primary_key)
        # 双括号{{ }}来表示'{shard}'和'{replica}'是作为字符串文本插入的固定值。

    # 执行SQL语句
    try:
        clickhouse_cursor = clickhouse_conn.execute(create_statement)
    except Exception as e:
        logging.error(f"执行SQL语句失败：{create_statement}")
        # logging.error(f"错误信息：{e}")

    # 输出ClickHouse表结构
    # print(f"ClickHouse create statement: {create_statement}")


def convert_mysql_database_to_clickhouse(mysql_conn, clickhouse_conn, excluded_databases=(
"mysql", "sys", "information_schema", "performance_schema", "test")):
    """
    将MySQL实例下的所有数据库表结构转换为ClickHouse
    """
    # 获取MySQL实例下所有数据库
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("SHOW DATABASES")
    databases = mysql_cursor.fetchall()

    # 遍历所有数据库和其中的表进行转换，排除掉被过滤的数据库
    for database in databases:
        database_name = database[0]
        if database_name not in excluded_databases:
            # 创建相应的ClickHouse数据库，请确保ClickHouse的账户权限正确
            try:
                if 'clickhouse_cluster_name' in globals() and TABLE_ENGINE == 'ReplicatedMergeTree':
                    create_database_statement = f"CREATE DATABASE IF NOT EXISTS {database_name} ON CLUSTER {clickhouse_cluster_name}"
                else:
                    create_database_statement = f"CREATE DATABASE IF NOT EXISTS {database_name}"
                clickhouse_conn.execute(create_database_statement)
            except Exception as e:
                logging.error(f"创建ClickHouse数据库{database_name}失败：{e}")
                raise

            # 切换到当前数据库
            mysql_cursor.execute(f"USE {database_name}")

            # 获取当前数据库下的所有表
            mysql_cursor.execute("SHOW TABLES")
            tables = mysql_cursor.fetchall()

            # 遍历所有表进行转换
            for table in tables:
                table_name = table[0]
                convert_mysql_to_clickhouse(mysql_conn, database_name, table_name, clickhouse_conn, database_name)


if __name__ == "__main__":
    # 连接MySQL数据库
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    # 连接ClickHouse数据库
    clickhouse_conn = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    # 转化表结构（将MySQL实例下的所有库 转换到 ClickHouse实例的相应库下）
    convert_mysql_database_to_clickhouse(mysql_conn, clickhouse_conn)
