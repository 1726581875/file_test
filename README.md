实现一个简单数据库，支持最简单的增删查改功能。

支持简单DDL语句：

```mysql
#查看所有数据库
show databases;

#创建数据库
create database xmz_db;

#删除数据库
drop database xmz_db;

#进入数据库
use xmz_db;

#查看当前数据库所有表
show tables;

#简单建表命令
create table test_table (id int, name varchar(10), time timestamp);

#查询表详情
desc test_table;

#删除表
drop table test_table;

#创建索引
create index idx_id on test_table(id);
alter table test_table add index idx_id(id);

#删除索引
drop index idx_id on test_table;

```



支持简单DML语句：

```mysql
#插入语句
insert into test_table(id,name,time) value (1,'张三','2023-05-19 00:00:00');

#更新语句
update test_table set name = '李四' where id = 1;

#删除语句
delete from test_table where id = 1; 

#查询语句
select * from test_table;

#支持简单聚合函数count、sum、max、min
select count(*),sum(id),max(id),min(id) from test_table;


#支持简单group by查询
select id,count(*) from test_table group by id;
#支持简单order by查询
select * from test_table order by id desc,name asc;


#支持简单子查询
select * from test_table where id in (select id from test_table);

select * from (select * from test_table) t;

#支持简单多表左连接、右连接、内连接查询
select * from table_a a right join table_b b on a.id = b.id;
select * from table_a a left join table_b b on a.id = b.id;
select * from table_a a inner join table_b b on a.id = b.id;

```



目前不支持事务、没有并发控制、没有缓冲区管理，甚至没有优化器。一切都非常简陋，只实现解析简单SQL到转换为操作指令再直接操作磁盘文件。

后续可以继续完善

1、事务的实现（undo log）

2、并发控制(隔离级别、各种锁的管理)

3、读写磁盘中间加个缓冲区(减少磁盘读取次数)

4、系统崩溃后恢复数据（redo log）

5、优化器的完善(目前只实现了简单条件表达式优化，条件下推)
