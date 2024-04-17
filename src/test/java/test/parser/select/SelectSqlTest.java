package test.parser.select;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.BaseSqlTest;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
@TestModule("简单select语句测试")
public class SelectSqlTest extends BaseSqlTest {

    private final static String databaseName = "select_test";

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

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
        testExecSQL("select * from (select '李华' as name) as t");
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


    @Override
    protected Database initDatabase() {
        return createDatabase(databaseName);
    }

}
