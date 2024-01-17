package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.SqlParser;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;

import java.util.concurrent.CountDownLatch;

/**
 * @author xiaomingzhang
 * @date 2023/7/16
 */
public class MutiThreadTest {


    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "yuany";

    private static Database database = null;


    static {
/*        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(databaseName);
        createDatabaseCommand.execute();*/
        database = Database.getDatabase(databaseName);
    }

    public static void main(String[] args) throws InterruptedException {
        testExecSQL("drop table if exists xmz_table_1");
        testExecSQL("create table xmz_table_1(id int, name varchar(10), time timestamp) ENGINE=" + engineType);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        InsertThread insertThread1 = new InsertThread(countDownLatch);
        InsertThread insertThread2 = new InsertThread(countDownLatch);
        InsertThread insertThread3 = new InsertThread(countDownLatch);

        new Thread(insertThread1, "thread-1").start();
        new Thread(insertThread2, "thread-2").start();
        new Thread(insertThread3, "thread-3").start();

        countDownLatch.await();

        System.out.println("========== 最终结果 =========");
        testExecSQL("select count(*) from xmz_table_1");
    }


    static class InsertThread implements Runnable {

        private CountDownLatch countDownLatch;

        public InsertThread(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                for(int i = 0; i < 100; i++) {
                    testExecSQL("insert into xmz_table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
                }

                System.out.println(Thread.currentThread().getName() + ":");
                testExecSQL("select count(*) from xmz_table_1");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }




    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }

}
