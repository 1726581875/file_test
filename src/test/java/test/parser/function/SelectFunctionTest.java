package test.parser.function;

import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.BaseSqlTest;

/**
 * @author xiaomingzhang
 * @date 2024/4/17
 */
@TestModule("查询函数测试")
public class SelectFunctionTest extends BaseSqlTest {

    private static final String dbName = "function_test";

    @Override
    protected Database initDatabase() {
        return createDatabase(dbName);
    }

    public static void main(String[] args) {
        SelectFunctionTest functionTest = new SelectFunctionTest();
        functionTest.testExecSQL("select to_timestamp(now())");
        functionTest.testExecSQL("select UNIX_TIMESTAMP(now()) as now");
        functionTest.case01();
    }


    @TestCase("测试案例1")
    public void case01() {
        testExecSQL("select now_timestamp()");
        testExecSQL("select now()");
        testExecSQL("select to_timestamp('2024-04-17 17:00:01')");
        testExecSQL("select to_timestamp(now())");
        testExecSQL("select UNIX_TIMESTAMP(now()) as now");
    }


}
