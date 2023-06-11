package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.session.ConnectSession;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/6/9
 */
public class TotalSqlTest {

    public static void main(String[] args) {
/*        testDatabaseDDL();
        testTableDDL();
        testInsert();*/
        testSimpleSelect();
    }

    public static void testSimpleSelect() {

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

        testExecSQL("drop table xmz_table");
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
