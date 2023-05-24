# binlog_parse_sql
解析MySQL binlog日志，将日志中的SQL语句提取出来，并输出成可执行的SQL文件，将其存入其他MySQL数据库

```shell> pip3 install pymysql mysql-replication  -i   "http://mirrors.aliyun.com/pypi/simple" --trusted-host "mirrors.aliyun.com"```

```shell> nohup python3 binlog_parse_sql.py &```
