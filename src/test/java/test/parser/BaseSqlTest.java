package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;

/**
 * @author xiaomingzhang
 * @date 2024/3/7
 */
public abstract class BaseSqlTest {

    protected Database database;

    protected abstract Database initDatabase();


    protected void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        Database db = getDatabase();
        ConnectSession connectSession = new ConnectSession(db);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.exec();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }

    private Database getDatabase() {
        if (this.database == null) {
            this.database = initDatabase();
        }
        return this.database;
    }

    protected Database createDatabase(String databaseName) {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.exec();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.exec();
        return Database.getDatabase(databaseName);
    }


}
