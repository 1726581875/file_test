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


    public static void main(String[] args) {
        yanStoreEngineTest();
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
