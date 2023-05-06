package com.moyu.test;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class LockTest {
    public static void main(String[] args) {

        for (int i = 0; i < 10; i++) {
            int finalI = i;
            String lockString = null;
            if(i < 5) {
                 lockString = new String("1111");
            } else {
                lockString = new String("2222");
            }
            String finalLockString = lockString;
            new Thread(() -> doSomething(finalLockString.intern(), "线程" + finalI)).start();
        }

    }


    public static void doSomething(String lockStr, String name) {

        System.out.println(name + "等待,lockStr=" + lockStr);
        synchronized (lockStr) {
            System.out.println(name + "获得锁,lockStr=" + lockStr);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

}
