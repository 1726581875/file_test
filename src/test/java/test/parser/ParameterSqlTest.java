package test.parser;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.DropDatabaseCommand;
import com.moyu.test.command.dml.sql.Parameter;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.terminal.util.PrintResultUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/22
 */
public class ParameterSqlTest {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "parameter_sql_test";

    private static Database database = null;

    static {
        DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand(databaseName, true);
        dropDatabaseCommand.execute();
        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(databaseName);
        createDatabaseCommand.execute();
        database = Database.getDatabase(databaseName);
    }

    public static void main(String[] args) {
        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);


        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma1', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma2', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma3', '2022-06-29 18:00:00')");


        Parameter parameter = new Parameter(1, 1);
        testExecSQL("select * from xmz_q_2 where id = ?", Arrays.asList(parameter));


        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(1, 12));
        parameters.add(new Parameter(2, "xmz"));
        parameters.add(new Parameter(3, new Date()));
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (?, ?, ?)", parameters);


        testExecSQL("select * from xmz_q_2");


        List<Parameter> updateParams = new ArrayList<>();
        updateParams.add(new Parameter(1, "aaaa"));
        updateParams.add(new Parameter(2, new Date()));
        updateParams.add(new Parameter(3,12));
        testExecSQL("update xmz_q_2 set name=?,time=? where 1 = 1 and id = ?", updateParams);

        testExecSQL("select * from xmz_q_2");

    }

    private static void testExecSQL(String sql) {
        testExecSQL(sql, new ArrayList<>());
    }

    private static void testExecSQL(String sql, List<Parameter> parameterList) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        AbstractCommand command = (AbstractCommand) connectSession.prepareCommand(sql);
        command.setParameterValues(parameterList);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }



}
