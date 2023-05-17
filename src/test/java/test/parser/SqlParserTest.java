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

/*        System.out.println("=== show databases ===");
        testExecSQL("show databases");

        System.out.println("=== create table ===");
        testExecSQL("create table xmz_aaa (id int, name varchar(10))");

        System.out.println("=== show tables ===");
        testExecSQL("show tables");

        System.out.println("=== desc table ===");
        testExecSQL("desc xmz_aaa");

        System.out.println("=== drop table ===");
        testExecSQL("drop table xmz_aaa");

        System.out.println("=== show tables ===");
        testExecSQL("show tables");


        System.out.println("=== create table ===");
        testExecSQL("create table xmz_aaa (id int, name varchar(10))");*/


/*        System.out.println("=== insert into table ===");
        testExecSQL("insert into  xmz_table (id, name) value (2, '啊啊啊啊22');");*/

        System.out.println("=== select * from table ===");
        testExecSQL("select * from xmz_table");
    }


    private static void testExecSQL(String sql) {
        ConnectSession connectSession = new ConnectSession("xmz", 0);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
        Arrays.asList(exec).forEach(System.out::println);
    }

}
