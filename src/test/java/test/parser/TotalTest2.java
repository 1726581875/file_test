package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/7/3
 */
public class TotalTest2 {


    private static final Integer databaseId = 3;
    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;
    //private static final String engineType = CommonConstant.ENGINE_TYPE_YU;


    public static void main(String[] args) {
        //yanStoreEngineTest();
        testCreateIndexCommand();
    }

    private static void testCreateIndexCommand(){
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
        
        testExecSQL("select count(*) from xmz_q_2");
        testExecSQL("select * from xmz_q_2 where id = 5");

        testExecSQL("create index idx_id on xmz_q_2(id)");

        testExecSQL("select * from xmz_q_2 where id = 5");
        testExecSQL("select * from xmz_q_2 where id = 11");
    }


    private static void yanStoreEngineTest() {
        testExecSQL("drop table if exists xmz_table");
        testExecSQL("create table xmz_table (id int primary key, name varchar(10)) ENGINE=" + engineType);
        testExecSQL("insert into  xmz_table (id, name) value (1, null);");
        testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
        testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
        testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

        testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '摸鱼');");

        testExecSQL("select * from xmz_table where ((name = '摸鱼') and (name = '摸鱼') and (name = '摸鱼'))");
        testExecSQL("select * from xmz_table where ((name = '摸鱼'))");
        testExecSQL("select count(*) from xmz_table where 1 = 1");
        testExecSQL("select * from xmz_table where 1 = 1");
        //testExecSQL("select * from xmz_table where '1'= '1'");



        //testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        testExecSQL("select * from xmz_table where (((name = '摸鱼') and (name = '摸鱼') and (name = '摸鱼')) or 1 = 1)");
    }

    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession("xmz", databaseId);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
        System.out.println("执行结果:");
        Arrays.asList(exec).forEach(System.out::println);
        System.out.println("====================================");
    }

}
