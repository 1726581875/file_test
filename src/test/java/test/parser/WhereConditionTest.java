package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class WhereConditionTest {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "yua";

    private static Database database = null;


    static {
/*        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(databaseName);
        createDatabaseCommand.execute();*/
        database = Database.getDatabase(databaseName);
    }

    public static void main(String[] args) {

        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);


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

        //testExecSQL("update xmz_q_2 set name = '522' where name not like 'Emma%'");
        testExecSQL("update xmz_q_2 set name = '522' where id in(1,2,3,5)");

        //testExecSQL("select * from xmz_q_2 where 1 = 1");
//        testExecSQL("update xmz_q_2 set id = 2 where id = 1 and id = 2 and id = 3");
//        testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 )");
        //testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 ) or name = '1111'");
        //testExecSQL("update xmz_q_2 set id = 2 where (id = 1 and id = 2) or (name = '1111' and name = '33')");
        //testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 ) or name = '1111'");
        testExecSQL("select count(*) from xmz_q_2 where (((1=1) or 1=1))");
    }


    private static void testExecSQL(String sql) {
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
