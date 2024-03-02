package test;

import test.annotation.TestCase;
import test.annotation.TestModule;

/**
 * @author xiaomingzhang
 * @date 2024/2/28
 */
@TestModule("测试模块")
public class MyTest {

    @TestCase("我的测试案例01")
    public void test(){

        System.out.println("执行了测试111111");
    }

}
