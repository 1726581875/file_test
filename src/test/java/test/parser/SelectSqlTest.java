package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;
import test.annotation.TestCase;
import test.annotation.TestModule;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
@TestModule("简单select语句测试")
public class SelectSqlTest {

    private final static String databaseName = "select_test";

    private static Database database = null;

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    static {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.exec();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.exec();
        database = Database.getDatabase(databaseName);
    }

    public static void main(String[] args) {
        SelectSqlTest sqlTest = new SelectSqlTest();
        sqlTest.selectNonTableTest();
    }

    @TestCase("仅仅使用select关键字查询")
    public void selectNonTableTest() {
        testExecSQL("select '我是     常量' AS aa,   'aaa aa';");
        testExecSQL("select '我是常量';");
        testExecSQL("select 100;");
        testExecSQL("select 100.01;");
        testExecSQL("select now();");
        testExecSQL("select uuid();");
        testExecSQL("select now(),uuid();");
        testExecSQL("select '啊啊啊',now(),uuid();");
        testExecSQL("select ';';");
        testExecSQL("select '; ';");
        testExecSQL("select ';;;;';");
    }

    @TestCase("selectTestCase1")
    public void selectTestCase1() {
        testExecSQL("drop table if exists  case_01");
        testExecSQL("create table case_01 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (3, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (4, 'Daniel', '2023-06-29 15:45:00')");

        testExecSQL("select a.id,a.name,a.time,'12 3 ' as t, 567 a, null from case_01 a;");

    }


    private void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.exec();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }


}
