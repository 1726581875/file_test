package test.parser.alterTable;

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
import test.parser.SelectSqlTest;

/**
 * @author xiaomingzhang
 * @date 2024/3/29
 */
@TestModule("修改表字段结构测试")
public class AlterTableColumnTest {

    private final static String databaseName = "alt_tb_column";

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
        AlterTableColumnTest sqlTest = new AlterTableColumnTest();
        sqlTest.alterTestCase1();
    }

    @TestCase("alter table add column添加字段测试")
    public void alterTestCase1() {
        testExecSQL("drop table if exists  case_01");
        testExecSQL("create table case_01 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (3, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO case_01 (id, name, time) VALUES (4, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("select * from case_01");
        testExecSQL("alter table case_01 add column state varchar(10) default '1';");
        testExecSQL("select * from case_01");
        testExecSQL("desc case_01");

        testExecSQL("alter table case_01 drop column name;");
        testExecSQL("select * from case_01");
        testExecSQL("alter table case_01 drop column state;");
        testExecSQL("select * from case_01");
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
