package test;

import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.terminal.util.PrintResultUtil;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.MutiThreadTest;
import test.parser.TotalSqlTest;
import test.parser.WhereConditionTest;
import test.parser.alterTable.AlterTableColumnTest;
import test.parser.createTable.CreateTableSqlTest;
import test.parser.joinTable.JoinTableTest;
import test.parser.select.SelectSqlTest;
import test.parser.subQuery.SubQueryTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xiaomingzhang
 * @date 2024/2/28
 * 总测试类
 */
public class TotalTestRunner {

    private static final List<TestResult> testResultList = new ArrayList<>();

    public static void main(String[] args) {
        // 测试建表语句
        runTestCase(CreateTableSqlTest.class);
        // 测试alter table语句
        runTestCase(AlterTableColumnTest.class);
        // 子查询测试
        runTestCase(SubQueryTest.class);
        // 综合测试
        runTestCase(TotalSqlTest.class);
        // 多线程测试
        runTestCase(MutiThreadTest.class);
        // 多表连接测试
        runTestCase(JoinTableTest.class);
        // 简单select语法测试
        runTestCase(SelectSqlTest.class);
        // where条件测试
        runTestCase(WhereConditionTest.class);

        System.out.println();
        System.out.println("############## 汇总结果 ################");

        // 生成简单测试报告
        List<String> headers = Arrays.asList("序号", "类名", "方法名", "测试模块", "测试案例", "测试结果");
        exportTestReport(headers, testResultList);
        printTestResult(headers, testResultList);
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

        if (objInstance == null) {
            System.err.println("测试类" + className + "创建实例失败，请确保有空构造方法");
            return;
        }

        Method[] declaredMethods = testClass.getDeclaredMethods();

        String template = "执行测试方法%s，%s";
        if (declaredMethods != null) {
            for (Method method : declaredMethods) {
                TestCase testCaseAnn = method.getAnnotation(TestCase.class);
                if (testCaseAnn != null) {
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

                    TestResult testResult = new TestResult(testResultList.size() + 1, className, methodName, moduleName, testCaseAnn.value());
                    if (isSuccess) {
                        System.out.println(String.format(template, className + "." + methodName, "成功"));
                        testResult.setResult("成功");
                    } else {
                        System.err.println(String.format(template, className + "." + methodName, "失败"));
                        testResult.setResult("失败");
                    }
                    testResultList.add(testResult);
                }
            }
        }
    }


    private static void exportTestReport(List<String> headers, List<TestResult> testResultList) {
        String headerStr = headers.stream().collect(Collectors.joining(","));
        StringBuilder testReportBuilder = new StringBuilder(headerStr + "\n");
        for (int i = 0; i < testResultList.size(); i++) {
            TestResult testResult = testResultList.get(i);
            testReportBuilder.append(testResult.getCsvString() + "\n");
        }
        File file = new File("tes_result.csv");
        FileOutputStream dos = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            dos = new FileOutputStream(file);
            dos.write(testReportBuilder.toString().getBytes());
            dos.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dos != null) {
                try {
                    dos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * @param headers
     * @param testResultList
     */
    private static void printTestResult(List<String> headers, List<TestResult> testResultList) {
        QueryResult queryResult = new QueryResult();
        // 表头: "序号", "类名", "方法名", "测试模块", "测试案例", "测试结果"
        SelectColumn[] selectColumns = new SelectColumn[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            selectColumns[i] = SelectColumn.newColumn(headers.get(i), DbTypeConstant.CHAR);
        }
        queryResult.setSelectColumns(selectColumns);
        // 数据行
        for (int i = 0; i < testResultList.size(); i++) {
            TestResult r = testResultList.get(i);
            queryResult.addRow(new Object[]{i + 1, r.getClassName(),  r.getMethodName()
                    , r.getModuleName(), r.getCaseName(), r.getResult()});
        }

        PrintResultUtil.printResult(queryResult);
    }


    static class TestResult {

        /**
         * 序号
         */
        private int num;
        /**
         * 类名
         */
        private String className;
        /**
         * 方法名
         */
        private String methodName;
        /**
         * 测试模块名
         */
        private String moduleName;
        /**
         * 测试案例名
         */
        private String caseName;
        /**
         * 测试结果
         */
        private String result;

        public TestResult() {

        }

        public TestResult(int num, String className, String methodName, String moduleName, String caseName) {
            this.num = num;
            this.className = className;
            this.methodName = methodName;
            this.moduleName = moduleName;
            this.caseName = caseName;
        }

        public int getNum() {
            return num;
        }

        public void setNum(int num) {
            this.num = num;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public String getModuleName() {
            return moduleName;
        }

        public void setModuleName(String moduleName) {
            this.moduleName = moduleName;
        }

        public String getCaseName() {
            return caseName;
        }

        public void setCaseName(String caseName) {
            this.caseName = caseName;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }


        public String getCsvString() {
            return num + "," + className + "," + methodName + "," + moduleName + "," + caseName + "," + result;
        }
    }


}
