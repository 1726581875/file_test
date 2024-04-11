package test.parser.subQuery;

import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.BaseSqlTest;

/**
 * @author xiaomingzhang
 * @date 2024/4/11
 */
@TestModule("子查询测试")
public class SubQueryTest extends BaseSqlTest {

    @Override
    protected Database initDatabase() {
        return createDatabase("sub_query");
    }


    @TestCase("testSubQueryCase01")
    public void testSubQueryCase01() {
        testExecSQL("drop table  if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select * from xmz_yan) t");
        testExecSQL("select * from (select * from xmz_yan where id = 1) t");
        testExecSQL("select * from (select * from xmz_yan ) t where t.id = 1");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1 ) t0 ) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");
    }


    public static void main(String[] args) {
        SubQueryTest subQueryTest = new SubQueryTest();
        subQueryTest.testSubQueryCase01();
    }


}
