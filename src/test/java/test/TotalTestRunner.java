package test;

import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.createTable.CreateTableSqlTest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author xiaomingzhang
 * @date 2024/2/28
 */
public class TotalTestRunner {

    public static void main(String[] args) {
        runTestCase(MyTest.class);
        // 测试建表语句
        runTestCase(CreateTableSqlTest.class);
    }

    public static void runTestCase(Class<?> testClass) {
        // 获取测试模块名称
        TestModule moduleAnn = testClass.getAnnotation(TestModule.class);
        String className = testClass.getName();
        String moduleName = (moduleAnn == null || moduleAnn.value() == null) ? className : moduleAnn.value();
        Object objInstance = null;
        System.out.println("开始执行测试模块[" + moduleName + "] 类名" + className);
        try {
            objInstance = testClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        if(objInstance == null) {
            System.out.println("测试类"+ className +"创建实例失败，请确保有空构造方法");
            return;
        }

        Method[] declaredMethods = testClass.getDeclaredMethods();

        String template = "执行测试方法%s，%s";
        if(declaredMethods != null) {
            for (Method method : declaredMethods) {
                TestCase testCaseAnn = method.getAnnotation(TestCase.class);
                if(testCaseAnn != null) {
                    String methodName = method.getName();
                    boolean isSuccess = false;
                    try {
                        method.invoke(objInstance, null);
                        isSuccess = true;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    if(isSuccess) {
                        System.out.println(String.format(template, className + "." + methodName, "成功"));
                    } else {
                        System.out.println(String.format(template, className + "." + methodName, "失败"));
                    }
                }
            }
        }
    }
}
