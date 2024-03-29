#### 使用场景： 从MySQL8.0实时解析binlog并复制到ClickHouse，适用于将MySQL8.0迁移至ClickHouse（ETL抽数据工具）

#### 原理：

将解析 binlog 和执行 SQL 语句两个过程分别由两个线程来执行。

其中，解析 binlog 的线程每次解析完一个事件后通过队列将 SQL 语句传给 SQL 执行线程，
SQL 执行线程从队列中取出 SQL 语句并按顺序依次执行，这样就保证了 SQL 语句的串行执行。

-----------------------------------
#### ClickHouse 使用：

#### 1）安装： 

```shell> pip3 install clickhouse-driver pymysql mysql-replication -i "http://mirrors.aliyun.com/pypi/simple" --trusted-host "mirrors.aliyun.com"```

注：clickhouse_driver库需要调用ssl，由于python3.10之后版本不在支持libressl使用ssl，需要用openssl1.1.1版本或者更高版本

参见：python3.10编译安装报SSL失败解决方法

https://blog.csdn.net/mdh17322249/article/details/123966953

#### 2）MySQL表结构转换为ClickHouse表结构
``` shell> vim mysql_to_clickhouse_schema.py（修改脚本里的配置信息）```

##### 注：mysql_to_clickhouse_schema_test.py（该工具仅为单表测试使用）
#####     mysql_to_clickhouse_schema_all.py（该工具将MySQL实例下的所有库 转换到 ClickHouse实例的相应库下）

运行

``` shell> python3 mysql_to_clickhouse_schema.py```

原理：连接MySQL获取表结构schema，然后在ClickHouse里执行建表语句。

#### 3) MySQL全量数据迁移至ClickHouse步骤：
```shell> python3 mysql_to_clickhouse_sync.py --mysql_host 192.168.198.239 --mysql_port 3336 --mysql_user admin --mysql_password 123456 --mysql_db yourDB --clickhouse_host 192.168.176.204 --clickhouse_port 9000 --clickhouse_user 123456 --clickhouse_password 123456 --clickhouse_database yourDB --batch_size 1000 --max_workers 10```

##### 注：没有自增主键的表，采用LIMIT offset, limit分页方式拉取数据。默认并行10张表同时导出数据，每次轮询取1000条数据。

会在工具目录下，生成metadata.txt文件（将binlog文件名、位置点和GTID信息保存到metadata.txt文件中）

#### 4）（ETL抽数据工具）将MySQL8.0迁移至ClickHouse（增量）
``` shell> vim binlog_parse_clickhouse.py（修改脚本里的配置信息）```

前台运行

```shell> python3 binlog_parse_clickhouse.py -f config.yaml```

后台运行

```shell> nohup python3 binlog_parse_clickhouse.py -f config.yaml > from_mysql_to_clickhouse.log 2>&1 &```




