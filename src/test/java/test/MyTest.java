package test;

import test.annotation.TestCase;
import test.annotation.TestModule;

import java.io.File;
import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2024/2/28
 */
@TestModule("测试模块")
public class MyTest {

    @TestCase("我的测试案例01")
    public void test() throws IOException {


    }

    public static void main(String[] args) throws IOException {
        new MyTest().test();
    }

}
