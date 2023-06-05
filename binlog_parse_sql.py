import os
import pymysql
import signal
import atexit
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.event import GtidEvent


# 源 MySQL 数据库设置
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

# 目标 MySQL 数据库设置
target_mysql_settings = {
    "host": "192.168.198.239",
    "port": 3306,
    "user": "hcy",
    "passwd": "hechunyang",
    "database": "hcy",
    "charset": "utf8"
}

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
    try:
        with open('binlog_info.txt', 'r') as f:
            binlog_file, binlog_pos = f.read().strip().split('\n')
    except Exception as e:
        print('Load binlog position failure:', e)
        binlog_file, binlog_pos = "mysql-bin.000003", 4 # 设置默认值为 mysql-bin.000001 和 4

    return binlog_file, int(binlog_pos)

# 退出程序时保存当前的 binlog 文件名和位置点
def exit_handler(stream, current_binlog_file, binlog_pos):
    stream.close()
    save_binlog_pos(current_binlog_file, binlog_pos)

# 在程序被终止时保存当前的 binlog 文件名和位置点
def save_binlog_pos_on_termination(signum, frame):
    save_binlog_pos(current_binlog_file, binlog_pos)
    quit_program()

# 退出程序时保存当前的 binlog 文件名和位置点
def quit_program():
    stream.close()
    target_conn.close()
    exit(0)

# 建立连接
target_conn = pymysql.connect(**target_mysql_settings)

saved_pos = load_binlog_pos()

stream = BinLogStreamReader(
    connection_settings=source_mysql_settings,
    server_id=source_server_id,  
    blocking=True,
    resume_stream=True,
    #only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
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
    
    if isinstance(binlogevent, QueryEvent):
        sql = binlogevent.query
        try:
            with target_conn.cursor() as cursor:
                cursor.execute(sql)
                target_conn.commit()
                print(f"success to execute SQL: {sql}")
        except Exception as e:
            print(f"Failed to execute SQL: {sql}")
            print(f"Error message: {e}")
    else:
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                sql = "INSERT INTO {}({}) VALUES ({})".format(
                    f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                    '`' + '`,`'.join(list(row["values"].keys())) + '`',
                    ','.join(["'%s'" % str(i) for i in list(row["values"].values())])
                )
                print(sql)
                try:
                    with target_conn.cursor() as cursor:
                        cursor.execute(sql)
                        target_conn.commit()
                except Exception as e:
                    print(f"Failed to execute SQL: {sql}")
                    print(f"Error message: {e}")
    
            elif isinstance(binlogevent, UpdateRowsEvent):
                sql = "UPDATE {} SET {} WHERE {}".format(
                    f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                    ','.join(["`{}`='{}'".format(k, v) for k, v in row["after_values"].items()]),
                    ' AND '.join(["`{}`='{}'".format(k, v) for k, v in row["before_values"].items()])
                )
                print(sql)
    
                try:
                    with target_conn.cursor() as cursor:
                       cursor.execute(sql)
                       target_conn.commit()
                except Exception as e:
                    print(f"Failed to execute SQL: {sql}")
                    print(f"Error message: {e}")
    
            elif isinstance(binlogevent, DeleteRowsEvent):
                sql = "DELETE FROM {} WHERE {}".format(
                    f"{database_name}.{binlogevent.table}" if database_name else binlogevent.table,
                    ' AND '.join(["`{}`='{}'".format(k, v) for k, v in row["values"].items()])
                )
                print(sql)
    
                try:
                    with target_conn.cursor() as cursor:
                        cursor.execute(sql)
                        target_conn.commit()
                except Exception as e:
                    print(f"Failed to execute SQL: {sql}")
                    print(f"Error message: {e}")

        return binlogevent.packet.log_pos

# 循环遍历解析出来的行事件并存入SQL语句中
while True:
    try:
        for binlogevent in stream:
            current_binlog_file = os.path.basename(stream.log_file)
            binlog_pos = process_rows_event(binlogevent, stream)
            save_binlog_pos(current_binlog_file, binlog_pos)

    except KeyboardInterrupt:
        save_binlog_pos(current_binlog_file, binlog_pos)
        break

    except pymysql.err.OperationalError as e:
        print("MySQL Error {}: {}".format(e.args[0], e.args[1]))

# 在程序退出时保存 binlog 位置
atexit.register(exit_handler, stream, current_binlog_file, binlog_pos)

# 接收 SIGTERM 和 SIGINT 信号
signal.signal(signal.SIGTERM, save_binlog_pos_on_termination)
signal.signal(signal.SIGINT, save_binlog_pos_on_termination)

# 关闭连接
atexit.register(target_conn.close)
