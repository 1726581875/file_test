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
        testInsertSQL();
        testExecSQL("delete from xmz_table where name like '摸鱼'");
        //testExecSQL("delete from xmz_table");
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

         testExecSQL("insert into  xmz_table (id, name) value (1, '摸鱼1');");
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
