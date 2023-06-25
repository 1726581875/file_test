package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.command.dml.InsertCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.obj.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/9
 */
public class TotalSqlTest {

    public static void main(String[] args) {
        testDatabaseDDL();
        testTableDDL();
        testInsert();
        testSimpleSelect();
        testUseIndex();
        testJoinTable();
        testSubQuery();
        testFunction();
        testSubQuery2();
        testAlias();
        testSubQueryFunction();
        testSubQueryFunction2();
        testRangeQuery();
        testRangeIndexQuery();
    }


    private static void testRangeIndexQuery() {
        fastInsertData("xmz_o_1", 10000);

        testExecSQL("create index idx_o_id on xmz_o_1(id);");

        testExecSQL("select * from xmz_o_1 where id <= 10");
        testExecSQL("select * from xmz_o_1 where id < 10");
        testExecSQL("select * from xmz_o_1 where id >= 10 and id <= 100");
        testExecSQL("select * from xmz_o_1 where id >= 9998");

        testExecSQL("select count(*) from xmz_o_1 where id < 500");
        testExecSQL("select count(*) from xmz_o_1 where id >= 500");
        testExecSQL("select count(*) from xmz_o_1 where id < 0");
        testExecSQL("select count(*) from xmz_o_1 where id < -1");
        testExecSQL("select count(*) from xmz_o_1 where id < 666 and id > 555");

        testExecSQL("select count(*) from xmz_o_1 where id >= 9998");
        testExecSQL("select * from xmz_o_1 where id = 9998");
        testExecSQL("select count(*) from xmz_o_1 where id <= 10000");
        testExecSQL("select count(*) from xmz_o_1 where id >= 0");

    }



    private static void fastInsertData(String tableName, int rowNum) {
        testExecSQL("drop table if exists " + tableName);

        testExecSQL("create table "+ tableName +" (id int, name varchar(10), time timestamp)");
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        ConnectSession connectSession = new ConnectSession("xmz", 1);
        InsertCommand insertCommand = new InsertCommand(connectSession, tableName, null, null);
        for (int i = 1; i <= rowNum; i++) {
            Column[] columns = getColumns(i, "name_" + i);
            columnList.add(columns);
            if (i % 10000 == 0) {
                insertCommand.testWriteList(columnList);
                System.out.println("插入一万条记录耗时:" + (System.currentTimeMillis() - time) + "ms");
                time = System.currentTimeMillis();
                columnList.clear();
            }
        }
        testExecSQL("select count(*) from " + tableName);

        testExecSQL("desc xmz_5");
    }


    private static Column[] getColumns(Integer id, String name) {
        Column[] columns = new Column[3];
        // 字段11
        columns[0] = new Column("id", ColumnTypeEnum.INT.getColumnType(), 0, 4);
        columns[0].setIsPrimaryKey((byte) 1);
        columns[0].setValue(id);

        // 字段2
        columns[1] = new Column("name", ColumnTypeEnum.VARCHAR.getColumnType(), 1, 100);
        columns[1].setValue(name);
        // 字段3
        columns[2] = new Column("time", ColumnTypeEnum.TIMESTAMP.getColumnType(), 2, 8);
        columns[2].setValue(new Date());
        return columns;
    }

    public static void testRangeQuery(){
        testExecSQL("drop table if exists  xmz_y_3");
        testExecSQL("create table xmz_y_3 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_3(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (3,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (4,'444','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_y_3 where id < 3");
        testExecSQL("select count(*) from xmz_y_3 where id < 3");
        testExecSQL("select  *  from xmz_y_3 where id >= 3");
        testExecSQL("select  *  from xmz_y_3 where id > 3");
        testExecSQL("select  *  from xmz_y_3 where id <= 3");
        testExecSQL("select  *  from xmz_y_3 where id <= 3 and id > 1");
        testExecSQL("select  *  from xmz_y_3 where id between 1 and 2");
    }


    public static void testSubQueryFunction2(){
        testExecSQL("drop table if exists  xmz_y_2");
        testExecSQL("create table xmz_y_2 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_2(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (3,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (1,'444','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_y_2");
        testExecSQL("select count(*) from xmz_y_2");
        testExecSQL("select id,count(*) from xmz_y_2 group by id");
        testExecSQL("select * from (select id,count(*) from xmz_y_2 group by id) t");
        testExecSQL("select * from (select id,count(*),max(id) from xmz_y_2 group by id) t");
    }


    public static void testSubQueryFunction() {

        testExecSQL("drop table  if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select max(id),min(id),count(*) from xmz_yan) t");
        testExecSQL("select * from (select max(id) as a,min(id) as b,count(*) as c from xmz_yan) t");
        testExecSQL("select * from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");
        testExecSQL("select t.* from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");

        testExecSQL("select t.a,t.b from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");

    }


    private static void testAlias(){

        testExecSQL("drop table if exists xmz_y_1");
        testExecSQL("create table xmz_y_1(id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_1(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select y.* from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select * from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select id,name,time from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select id,name,time from xmz_y_1 where id in (1)");
        testExecSQL("select xmz.id,xmz.name,xmz.time from xmz_y_1 xmz");
        testExecSQL("select id as id1,name as name2 ,time as time3 from xmz_y_1 where id in (1)");
        testExecSQL("select bb.id as id1,bb.name as name2 ,bb.time as time3 from xmz_y_1 as bb");

        testExecSQL("select bb.id as id1, bb.* from xmz_y_1 as bb");
    }



    private static void testSubQuery2(){
        testExecSQL("drop table if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select * from xmz_yan) t");
        testExecSQL("select * from (select * from xmz_yan a where a.id = 1) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id where b.id = 1 ) t where id = 1");
        testExecSQL("select * from (select * from xmz_yan where id = 1) t");
        testExecSQL("select * from (select * from xmz_yan ) t where t.id = 1");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1 ) t0 ) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");


        testExecSQL("select * from xmz_yan where id in (1)");
        testExecSQL("select * from xmz_yan where name in ('222')");
        testExecSQL("select * from xmz_yan where name not in ('222')");
        testExecSQL("select * from xmz_yan where id in (select id from xmz_yan)");
    }



    private static void testFunction(){

        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("desc table_1");

        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from table_1");

        testExecSQL("update table_1 set name = 'aaa' where id = 1");

        testExecSQL("select * from table_1");


        testExecSQL("select count(*) from table_1");
        testExecSQL("select sum(id) from table_1");
        testExecSQL("select max(id) from table_1");
        testExecSQL("select min(id) from table_1");

        testExecSQL("select count(*),sum(id) from table_1");
        testExecSQL("select max(id),sum(id) from table_1");
        testExecSQL("select min(id),sum(id) from table_1");
        testExecSQL("select max(id),min(id) from table_1");

        testExecSQL("select count(time) from table_1 where id = 2");
        testExecSQL("select sum(time) from table_1 where id = 2");
        testExecSQL("select max(time) from table_1 where id = 2");
        testExecSQL("select min(time) from table_1 where id = 2");


        testExecSQL("truncate table table_1");

        testExecSQL("drop table if exists  table_1");
    }


    public static void testSubQuery() {

        testExecSQL("drop table  if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select * from xmz_yan) t");
        testExecSQL("select * from (select * from xmz_yan where id = 1) t");
        testExecSQL("select * from (select * from xmz_yan ) t where t.id = 1");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1 ) t0 ) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");


        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");


    }


    public static void testJoinTable() {

        testExecSQL("drop table  if exists  xmz_00");
        testExecSQL("create table xmz_00 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_00(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_00(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_00(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("drop table xmz_01");
        testExecSQL("create table xmz_01 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_01(id,name,time) value (1,'111.','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_01(id,name,time) value (4,'444.','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_01(id,name,time) value (5,'555.','2023-05-19 00:00:00')");


        testExecSQL("drop table xmz_02");
        testExecSQL("create table xmz_02 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_02(id,name,time) value (1,'111.2','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_02(id,name,time) value (3,'222.2','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_00 as a inner join xmz_01 as b on a.id = b.id inner join xmz_02 as c on a.id = c.id");


        testExecSQL("select * from xmz_00 a right join xmz_01 b on a.id = b.id");
        testExecSQL("select * from xmz_00 a left join xmz_01 b on a.id = b.id");
        testExecSQL("select * from xmz_00 a inner join xmz_01 b on a.id = b.id");
    }



    public static void testUseIndex() {
        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (3,'333','2023-05-19 00:00:00')");



        testExecSQL("create index id_idx on xmz_table_1(id)");

        testExecSQL("select * from xmz_table_1 where id = 3");


        testExecSQL("drop table if exists xmz_table_1");
    }



    public static void testSimpleSelect() {

        testExecSQL("drop table if exists xmz_table");
        testExecSQL("create table xmz_table (id int, name varchar(10))");

        testExecSQL("insert into  xmz_table (id, name) value (1, null);");
        testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
        testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
        testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

        testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '摸鱼');");

        testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' and name = '摸鱼123')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' or name = '摸鱼')");
        testExecSQL("select * from xmz_table");
        testExecSQL("select * from xmz_table where name = '摸鱼' or (id = 1)");
        testExecSQL("select * from xmz_table where (((name = '摸鱼') or (id = 1)))");

        testExecSQL("select * from xmz_table where name like 摸鱼");
        testExecSQL("select * from xmz_table where name is null");
        testExecSQL("select * from xmz_table where name is not null");


    }


    public static void testInsert() {
        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (3,'333','2023-05-19 00:00:00')");
        testExecSQL("drop table xmz_table_1");
    }


    private static void testTableDDL() {
        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_2 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_3 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_4 (id int, name varchar(10), time timestamp)");
        testExecSQL("drop table xmz_table_2");
        testExecSQL("show tables");

        testExecSQL("desc xmz_table_3");
        testExecSQL("drop table xmz_table_1");
        testExecSQL("drop table xmz_table_3");
        testExecSQL("drop table xmz_table_4");
    }


    private static void testDatabaseDDL() {
        testExecSQL("show databases");
        testExecSQL("create database xmz_01");
        testExecSQL("create database xmz_02");
        testExecSQL("create database xmz_03");
        testExecSQL("drop database xmz_02");
        testExecSQL("show databases");
        testExecSQL("drop database xmz_01");
        testExecSQL("drop database xmz_03");
    }



    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession("xmz", 1);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
        System.out.println("执行结果:");
        Arrays.asList(exec).forEach(System.out::println);
        System.out.println("====================================");
    }

}
