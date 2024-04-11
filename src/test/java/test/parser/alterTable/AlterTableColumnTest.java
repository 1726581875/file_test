package test.parser.alterTable;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.BaseSqlTest;

/**
 * @author xiaomingzhang
 * @date 2024/3/29
 */
@TestModule("修改表字段结构测试")
public class AlterTableColumnTest extends BaseSqlTest {

    private final static String databaseName = "alt_tb_column";

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    @Override
    protected Database initDatabase() {
        return createDatabase(databaseName);
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





}
