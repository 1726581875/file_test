package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;

/**
 * @author xiaomingzhang
 * @date 2023/9/14
 */
public class ColumnTypeTest {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "column_test";

    private static Database database = null;


    static {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.execCommand();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.execCommand();
        database = Database.getDatabase(databaseName);
    }

    public static void main(String[] args) {
        // 测试tinyint类型
        testExecSQL("drop table if exists column_test_1");
        testExecSQL("create table column_test_1 (id int, name varchar(10), time timestamp, state tinyint)");
        testExecSQL("insert into column_test_1 (`id`, `name`, `time`, `state`) values (1, 'sanji', '2023-09-14 10:43:00', 1)");
        testExecSQL("insert into column_test_1 values (2, 'zr', '2023-09-14 10:43:00', 0)");
        testExecSQL("select * from column_test_1");

        // 测试datetime类型
        testExecSQL("drop table if exists column_test_datetime");
        testExecSQL("create table column_test_datetime (time datetime)");
        testExecSQL("insert into column_test_datetime (time) values ('2023-09-14 10:43:00')");
        testExecSQL("insert into column_test_datetime (time) values ('22-09-14 10:43:00')");

        testExecSQL("select * from column_test_datetime");
    }


    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }


}
