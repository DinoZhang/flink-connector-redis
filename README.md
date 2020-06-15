# flink-connector-redis
基于`Blink`的`Flink SQL Redis`连接器，支持`Redis`维表和结果表。

[![HitCount](http://hits.dwyl.com/DinoZhang/DinoZhang/flink-connector-redis.svg)](http://hits.dwyl.com/DinoZhang/DinoZhang/flink-connector-redis)



## 安装
```shell
    mvn clean package -Pfast
```
打包完成后，将`flink-jdbc-1.5.1.jar`和`jedis-2.7.2.jar`拷贝至`Fink lib`目录下。
## 使用
### 维表
```sql
create table redis_dim (
  k varchar,
  v varchar,
  primary key(k)
) with (
  type = 'redis',
  host = '127.0.0.1',
  port = '6379',
  password = ''
);
```
### 结果表
支持String类型，等价于`set key value`。
```sql
create table redis_sink (
  k varchar,
  v varchar,
  primary key(k)
) with (
  type = 'redis',
  host = '127.0.0.1',
  port = '6379',
  password = '',  
  db = '0'
);
```
### 示例
```sql
create table kafka_source (
  messageKey varbinary,
  message varbinary,
  topic varchar,
  `partition` int,
  `offset` bigint
) with (
  type = 'kafka010',
  topic = 'test',
  bootstrap.servers = 'localhost:9092',
  `group.id` = 'sql_group_1'
);

create table redis_sink (
  k varchar,
  v varchar,
  primary key(k)
) with (
  type = 'redis',
  host = '127.0.0.1',
  port = '6379',
  password = '',  
  db = '0'
);

insert into redis_sink
 select cast(`offset` as varchar),cast(CURRENT_TIMESTAMP as varchar) from kafka_source;

```
 
