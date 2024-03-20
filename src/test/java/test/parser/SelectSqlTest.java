package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
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
/*        testExecSQL("select '我是常量';");
        testExecSQL("select 100;");
        testExecSQL("select now();");
        testExecSQL("select uuid();");
        testExecSQL("select now(),uuid();");
        testExecSQL("select '啊啊啊',now(),uuid();");*/
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
