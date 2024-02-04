#### 使用场景：
#### 1）从MySQL8.0实时解析binlog并复制到ClickHouse，适用于将MySQL8.0迁移至ClickHouse（ETL抽数据工具）

#### 原理：

将解析 binlog 和执行 SQL 语句两个过程分别由两个线程来执行。

其中，解析 binlog 的线程每次解析完一个事件后通过队列将 SQL 语句传给 SQL 执行线程，
SQL 执行线程从队列中取出 SQL 语句并按顺序依次执行，这样就保证了 SQL 语句的串行执行。

-----------------------------------
#### ClickHouse使用：

1）安装： 

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

a) ```/usr/bin/mydumper -h 192.168.192.180 -u hechunyang -p 123456 -P 3306 --no-schemas -t 12 --csv -v 3 --regex '^hcy.user$' -o ./```

注：需要mydumper 0.12.3-3版本支持导出CSV格式

b) ```clickhouse-client --query="INSERT INTO hcy.user FORMAT CSV" < hcy.user.00000.dat```

### c) 或者使用mysql_to_clickhouse_sync.py工具（MySQL全量数据导入到ClickHouse里，默认并行10张表同时导出数据，每次轮询取1000条数据。）

##### 使用条件：表必须有自增主键，测试环境MySQL 8.0

##### 如果你说服不了开发对每张表增加自增主键ID，那么你要设置参数sql_generate_invisible_primary_key​​​
##### 开启这个参数，会在建表时，检查表中是否有主键，如果没有主键，则会自动创建。该参数非常实用，减少了DBA对sql语句表结构的审计。
##### 参考 https://blog.51cto.com/hcymysql/5952924

### d) mysql_to_clickhouse_sync_pagination.py工具（MySQL全量数据导入到ClickHouse里，主键ID可以不自增。）

##### 注：没有自增主键的表，采用LIMIT offset, limit分页方式拉取数据。

```shell> python3 mysql_to_clickhouse_sync.py --mysql_host 192.168.198.239 --mysql_port 3336 --mysql_user admin --mysql_password hechunyang --mysql_db hcy --clickhouse_host 192.168.176.204 --clickhouse_port 9000 --clickhouse_user hechunyang --clickhouse_password 123456 --clickhouse_database hcy --batch_size 1000 --max_workers 10```

会在工具目录下，生成metadata.txt文件（将binlog文件名、位置点和GTID信息保存到metadata.txt文件中）

#### 4）binlog_parse_clickhouse.py（ETL抽数据工具）将MySQL8.0迁移至ClickHouse（增量）
``` shell> vim binlog_parse_clickhouse.py（修改脚本里的配置信息）```

前台运行

```shell> python3 binlog_parse_clickhouse.py```

后台运行

```shell> nohup python3 binlog_parse_clickhouse.py > from_mysql_to_clickhouse.log 2>&1 &```




