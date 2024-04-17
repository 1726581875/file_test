package test.parser;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.Database;
import test.annotation.TestCase;
import test.annotation.TestModule;

import java.util.concurrent.CountDownLatch;

/**
 * @author xiaomingzhang
 * @date 2023/7/16
 */
@TestModule("多线程测试")
public class MutiThreadTest extends BaseSqlTest {


    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "yuany";


    @TestCase("mutiInsertTest")
    public void mutiInsertTest() {
        testExecSQL("drop table if exists xmz_table_1");
        testExecSQL("create table xmz_table_1(id int, name varchar(10), time timestamp) ENGINE=" + engineType);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        InsertThread insertThread1 = new InsertThread(countDownLatch);
        InsertThread insertThread2 = new InsertThread(countDownLatch);
        InsertThread insertThread3 = new InsertThread(countDownLatch);

        new Thread(insertThread1, "thread-1").start();
        new Thread(insertThread2, "thread-2").start();
        new Thread(insertThread3, "thread-3").start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("========== 最终结果 =========");
        testExecSQL("select count(*) from xmz_table_1");
    }


    public static void main(String[] args) throws InterruptedException {
        MutiThreadTest m = new MutiThreadTest();
        m.mutiInsertTest();
    }


    class InsertThread implements Runnable {

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


    @Override
    protected Database initDatabase() {
        return createDatabase(databaseName);
    }

}
