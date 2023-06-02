# binlog_parse_sql
解析MySQL binlog日志，将日志中的SQL语句提取出来，并输出成可执行的SQL文件，将其存入其他MySQL数据库

```shell> pip3 install pymysql mysql-replication  -i   "http://mirrors.aliyun.com/pypi/simple" --trusted-host "mirrors.aliyun.com"```

```shell> nohup python3 binlog_parse_sql.py &```

LookupError: unknown encoding: utf8mb3
解决方案：
https://github.com/julien-duponchelle/python-mysql-replication/issues/386

```
/usr/local/python3/lib/python3.10/site-packages/pymysql/charset.py文件，增加如下：
_charsets.add(Charset(256, "utf8mb3", "utf8mb3_general_ci", "Yes"))
_charsets.add(Charset(257, "utf8mb3", "utf8mb3_bin", ""))
```
