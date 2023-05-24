package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.command.dml.InsertCommand;
import com.moyu.test.session.ConnectSession;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class SqlParserTest {


    public static void main(String[] args) {

        testExecSQL("drop table table_1");

        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");

        testExecSQL("truncate table table_1");

        long beginTime = System.currentTimeMillis();

        long time = beginTime;
        for (int i = 1; i <= 10000; i++) {

            String insertSQL = "insert into table_1(id,name,time) value ("+ i + ",'name_"+ i +"','2023-05-19 00:00:00')";
            ConnectSession connectSession = new ConnectSession("xmz", 0);
            SqlParser sqlParser = new SqlParser(connectSession);
            InsertCommand command = (InsertCommand)sqlParser.prepareCommand(insertSQL);
            command.testWriteList();
            if(i % 1000 == 0) {
                System.out.println("插入一千条记录耗时:" + (System.currentTimeMillis() - time) / 1000 + "s");
                time = System.currentTimeMillis();
            }
        }

        System.out.println("总耗时:" + (System.currentTimeMillis() - beginTime) / 1000 + "s");
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
