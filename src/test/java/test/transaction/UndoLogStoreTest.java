package test.transaction;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.transaction.Transaction;
import com.moyu.test.store.transaction.TransactionManager;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/6/14
 */
public class UndoLogStoreTest {

    public static void main(String[] args) {

        ConnectSession connectSession = new ConnectSession("xmz", 1);
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)", connectSession);
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')", connectSession);
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')", connectSession);
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'333','2023-05-19 00:00:00')", connectSession);


        int tid = TransactionManager.initTransaction(connectSession);
        testExecSQL("update xmz_yan set name = '520' where id = 1", connectSession);
        testExecSQL("update xmz_yan set name = '250' where id = 2", connectSession);
        testExecSQL("insert into xmz_yan(id,name,time) value (4,'444','2023-05-19 00:00:00')", connectSession);
        testExecSQL("delete from xmz_yan where id = 1", connectSession);
        testExecSQL("select * from xmz_yan", connectSession);
        Transaction transaction = TransactionManager.getTransaction(tid);
        transaction.rollback();


        testExecSQL("select * from xmz_yan", connectSession);

        testExecSQL("drop table xmz_yan", connectSession);

    }

    private static Float getCompletionRate(Long total, Long completed) {
        Float completionRate = 0f;
        // 办结率
        if (total != 0L) {
            completionRate = Float.valueOf(completed) / total;
        }

        float rate = (float) Math.floor(completionRate * 100) / 100f;
/*        if (rate == 1f && total > completed) {
            rate = 0.99f;
        }*/
        return rate;
    }



    private static void testExecSQL(String sql, ConnectSession session) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        SqlParser sqlParser = new SqlParser(session);
        Command command = sqlParser.prepareCommand(sql);
        String[] exec = command.exec();
        System.out.println("执行结果:");
        Arrays.asList(exec).forEach(System.out::println);
        System.out.println("====================================");
    }


}
