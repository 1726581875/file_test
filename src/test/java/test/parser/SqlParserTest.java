package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.session.ConnectSession;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class SqlParserTest {


    public static void main(String[] args) {
        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("desc table_1");

        testExecSQL("insert into table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (3,'333','2023-05-19 00:00:00')");

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
