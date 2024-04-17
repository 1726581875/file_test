package test.parser;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
@TestModule("条件测试")
public class WhereConditionTest extends BaseSqlTest {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "condition_test";


    @Override
    protected Database initDatabase() {
        return createDatabase(databaseName);
    }

    public static void main(String[] args) {
        WhereConditionTest t = new WhereConditionTest();
        t.testCase01();
    }

    @TestCase("testCase01")
    public void testCase01(){
        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 )");
        testExecSQL("update xmz_q_2 set id = 2 where (id = 1 and id = 2)");
        testExecSQL("update xmz_q_2 set id = 2 where (     id = 1         and id          = 1        )");
        testExecSQL("update xmz_q_2 set id = 2 where (     id = 1         and id          = 1     or (1 = 1)   )");
    }


    @TestCase("simpleConditionTest")
    public void simpleConditionTest() {
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

        testExecSQL("update xmz_q_2 set name = '522' where name not like 'Emma%'");
        testExecSQL("update xmz_q_2 set name = '522' where id in(1,2,3,5)");

        testExecSQL("select * from xmz_q_2 where 1 = 1");
        testExecSQL("update xmz_q_2 set id = 2 where id = 1 and id = 2 and id = 3");
        testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 )");
        testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 ) or name = '1111'");
        testExecSQL("update xmz_q_2 set id = 2 where (id = 1 and id = 2) or (name = '1111' and name = '33')");
        testExecSQL("update xmz_q_2 set id = 2 where ( id = 1 and id = 2 ) or name = '1111'");
        testExecSQL("select count(*) from xmz_q_2 where (((1=1) or 1=1))");
    }


}
