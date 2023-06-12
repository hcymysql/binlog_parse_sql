![image](https://s2.51cto.com/images/202306/13bd1a445f0874cc06e869444d718a0f2cf32f.png?x-oss-process=image/watermark,size_14,text_QDUxQ1RP5Y2a5a6i,color_FFFFFF,t_30,g_se,x_10,y_10,shadow_20,type_ZmFuZ3poZW5naGVpdGk=/format,webp)

由于 MariaDB 已经远离 MySQL，从MySQL 8.0迁移到MariaDB 10.5时，binlog同步复制已经不兼容。

参考手册： https://mariadb.com/kb/en/mariadb-vs-mysql-compatibility/

![image](https://s2.51cto.com/images/202306/d9f040596cfa78d7ca3022f68dc63d9b19bdf7.png?x-oss-process=image/watermark,size_14,text_QDUxQ1RP5Y2a5a6i,color_FFFFFF,t_30,g_se,x_10,y_10,shadow_20,type_ZmFuZ3poZW5naGVpdGk=/format,webp)

1）若MariaDB是主库，MySQL是从库，在GTID模式下，从MariaDB同步复制数据时，GTID与MySQL不兼容，同步将报错。

2）若MySQL是主库，MariaDB是从库，MariaDB无法从MySQL 8.0主库上复制，因为MySQL 8.0具有不兼容的二进制日志格式。

需要借助binlog_parse_queue工具，将binlog解析并生成SQL语句，反向插入MariaDB数据库里。

#### 使用场景：

1）从MySQL8.0实时解析binlog并复制到MariaDB，适用于将MySQL8.0迁移至MariaDB（ETL抽数据工具）

2）数据恢复（研发手抖误删除一张表，通过历史全量恢复+binlog增量恢复）

#### 3）从MySQL8.0实时解析binlog并复制到ClickHouse，适用于将MySQL8.0迁移至ClickHouse（ETL抽数据工具）--binlog_parse_clickhouse.py

#### 原理：

将解析 binlog 和执行 SQL 语句两个过程分别由两个线程来执行。

其中，解析 binlog 的线程每次解析完一个事件后通过队列将 SQL 语句传给 SQL 执行线程，
SQL 执行线程从队列中取出 SQL 语句并按顺序依次执行，这样就保证了 SQL 语句的串行执行。

-----------------------------------
#### MariaDB使用：
1）安装： 

```shell> pip3 install pymysql mysql-replication -i "http://mirrors.aliyun.com/pypi/simple" --trusted-host "mirrors.aliyun.com"```

2）前台运行

```shell> python3 binlog_parse_queue.py```

![image](https://s2.51cto.com/images/202306/b3c971e530888984170795dda364cf2a683235.png?x-oss-process=image/watermark,size_14,text_QDUxQ1RP5Y2a5a6i,color_FFFFFF,t_30,g_se,x_10,y_10,shadow_20,type_ZmFuZ3poZW5naGVpdGk=/format,webp)

3）后台运行

```shell> nohup python3 binlog_parse_queue.py > from_mysql_to_mariadb.log 2>&1 &```

4）工具运行后，会生成binlog_info.txt文件，即实时保存已经解析过的binlog文件名和position位置点，以方便程序挂掉后的断点续传。


注：运行后如报错 ```LookupError: unknown encoding: utf8mb3```

解决方案

```
/usr/local/python3/lib/python3.10/site-packages/pymysql/charset.py文件，增加如下：
_charsets.add(Charset(256, "utf8mb3", "utf8mb3_general_ci", "Yes"))
_charsets.add(Charset(257, "utf8mb3", "utf8mb3_bin", ""))
```

参考如下链接：
https://github.com/julien-duponchelle/python-mysql-replication/issues/386

##############################################################################################
#### ClickHouse使用：
1）安装： 

```shell> pip3 install clickhouse-driver -i "http://mirrors.aliyun.com/pypi/simple" --trusted-host "mirrors.aliyun.com"```

注：clickhouse_driver库需要调用ssl，由于python3.10之后版本不在支持libressl使用ssl，需要用openssl1.1.1版本或者更高版本

参见：python3.10编译安装报SSL失败解决方法

https://blog.csdn.net/mdh17322249/article/details/123966953

2）#### MySQL表结构转换为ClickHouse表结构
``` shell> vim mysql_to_clickhose_schema.py（修改脚本里的配置信息）```

运行
``` shell> python3 mysql_to_clickhose_schema.py```

原理：连接MySQL获取表结构schema，然后在ClickHouse里执行建表语句。

3）#### binlog_parse_clickhouse.py（ETL抽数据工具）将MySQL8.0迁移至ClickHouse
``` shell> vim binlog_parse_clickhouse.py（修改脚本里的配置信息）```

前台运行

```shell> python3 binlog_parse_clickhouse.py```

后台运行

```shell> nohup python3 binlog_parse_clickhouse.py > from_mysql_to_clickhouse.log 2>&1 &```




