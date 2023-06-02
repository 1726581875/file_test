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
 * @date 2023/5/16
 */
public class SqlParserTest {


    public static void main(String[] args) {
        createIndexSqlTest();
    }


    private static void createIndexSqlTest(){
        testExecSQL("drop table xmz_3");

        testExecSQL("create table xmz_3 (id int, name varchar(10), time timestamp)");
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        InsertCommand insertCommand = new InsertCommand(0, "xmz_3", null, null);
        int rowNum = 10000;
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

        testExecSQL("select count(*) from xmz_3");

        testExecSQL("ALTER TABLE xmz_3 ADD PRIMARY KEY indexName(id);");

        testExecSQL("select * from xmz_3 where id = 1000");

        //testExecSQL("CREATE INDEX aaaa ON xmz_1 (id);");

        testExecSQL("desc xmz_3");
    }



    private static void testPrimaryKey(){
        testExecSQL("drop table table_2");

        testExecSQL("create table table_2 (id int PRIMARY KEY, name varchar(10), time timestamp)");

        testExecSQL("insert into table_2(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into table_2(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_2(id,name,time) value (3,'333','2023-05-19 00:00:00')");
        testExecSQL("desc table_2");

        testExecSQL("select * from table_2");
    }


    private static void batchInsertData() {
        testExecSQL("drop table table_1");

        testExecSQL("create table table_1 (id int PRIMARY KEY, name varchar(10), time timestamp)");
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        InsertCommand insertCommand = new InsertCommand(0, "table_1", null, null);
        int rowNum = 10000000;
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


        Column[] columns = getColumns(-1, "");
        time = System.currentTimeMillis();
        System.out.println("set Index:" + (System.currentTimeMillis() - time) / 1000 + "s");


        testExecSQL("select count(*) from table_1");
        System.out.println("总耗时:" + (System.currentTimeMillis() - beginTime) / 1000 + "s");
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






    private  static void dropTableTest() {
        testExecSQL("show tables");

        testExecSQL("create table xmz_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_2 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_3 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_4 (id int, name varchar(10), time timestamp)");

        testExecSQL("show tables");

        testExecSQL("drop table xmz_2");

        testExecSQL("show tables");
    }




    private static void testFunction(){
        testExecSQL("select count(*) from table_1");
        testExecSQL("select sum(id) from table_1");
        testExecSQL("select max(id) from table_1");
        testExecSQL("select min(id) from table_1");

        testExecSQL("select count(*),sum(id) from table_1");
        testExecSQL("select max(id),sum(id) from table_1");
        testExecSQL("select min(id),sum(id) from table_1");
        testExecSQL("select max(id),min(id) from table_1");

        testExecSQL("select count(time) from table_1 where id = 100000");
        testExecSQL("select sum(time) from table_1 where id = 100000");
        testExecSQL("select max(time) from table_1 where id = 100000");
        testExecSQL("select min(time) from table_1 where id = 100000");
    }


    private static void testInsert() {
        // testExecSQL("drop database xmz");

        testExecSQL("drop table table_1");

        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");

        testExecSQL("truncate table table_1");

        long beginTime = System.currentTimeMillis();

        long time = beginTime;
        for (int i = 1; i <= 100000; i++) {

            String insertSQL = "insert into table_1(id,name,time) value ("+ i + ",'name_"+ i +"','2023-05-19 00:00:00')";
            testExecSQL2(insertSQL);

            if(i % 1000 == 0) {
                System.out.println("插入一千条记录耗时:" + (System.currentTimeMillis() - time) / 1000 + "s");
                time = System.currentTimeMillis();
            }
        }

        System.out.println("总耗时:" + (System.currentTimeMillis() - beginTime) / 1000 + "s");
    }


    private static void testUpdateSQL() {
        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("desc table_1");

        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from table_1");

        testExecSQL("update table_1 set name = 'aaa' where id = 1");

        testExecSQL("select * from table_1");

        testExecSQL("truncate table table_1");
    }


    private static void testInsertTimeTypeValue(){
        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("desc table_1");

        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from table_1");

        testExecSQL("select * from table_1 where name is not null");
    }



    private static void testDelete(){
        testExecSQL("delete from xmz_table");
        testInsertSQL();
        testExecSQL("delete from xmz_table where name like '摸鱼'");
        testExecSQL("select * from xmz_table");
    }


    private static void testDDLSQL() {
        testExecSQL("show databases");

        testExecSQL("create table xmz_aaa (id int, name varchar(10))");

        testExecSQL("show tables");

        testExecSQL("desc xmz_aaa");

        testExecSQL("drop table xmz_aaa");

        testExecSQL("show tables");

        testExecSQL("create table xmz_aaa (id int, name varchar(10))");
    }

    private static void testInsertSQL() {

         testExecSQL("insert into  xmz_table (id, name) value (1, null);");
         testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
         testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
         testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

         testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
         testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");

    }


    private static void testSelectCondition(){
        testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' and name = '摸鱼123')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' or name = '摸鱼')");
        testExecSQL("select * from xmz_table");
        testExecSQL("select * from xmz_table where name = '摸鱼' or (id = 1)");
        testExecSQL("select * from xmz_table where (((name = '摸鱼') or (id = 1)))");
    }


    private static void testExecSQL2(String sql) {
        ConnectSession connectSession = new ConnectSession("xmz", 0);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
    }

    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession("xmz", 0);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
        System.out.println("执行结果:");
        Arrays.asList(exec).forEach(System.out::println);
        System.out.println("====================================");
    }

}
