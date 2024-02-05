package test;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.command.dml.SelectCmd;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.terminal.util.PrintResultUtil;
import com.moyu.xmz.common.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/6
 */
public class PrintResultTest {


    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;
    //private static final String engineType = CommonConstant.ENGINE_TYPE_YU;

    private final static String databaseName = "print_test";

    private static Database database = null;

    static {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.execCommand();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.execCommand();
        database = Database.getDatabase(databaseName);
    }


    public static void main(String[] args) {
/*
        String sql = "show databases";
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();


*/
       // System.out.println("[RECORD "+ (0 + 1) +"]");

        testPrint();
    }


    private static void testPrint(){

        testExecSQL("drop table if exists print_test");
        testExecSQL("create table print_test (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (11, 'Emma1', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (11, 'Emma2', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO print_test (id, name, time) VALUES (11, 'Emma3', '2022-06-29 18:00:00')");



        String sql = "select * from print_test";
        ConnectSession connectSession = new ConnectSession(database);
        SelectCmd command = (SelectCmd) connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");



        SelectColumn[] selectColumns = queryResult.getSelectColumns();
        List<Object[]> resultRows = queryResult.getResultRows();
        String[] columnNames = new String[selectColumns.length];
        // 获取最长字段名长度
        int maxCNameLen = 10;
        // 获取长的值长度
        int maxValueLen = 10;
        for (int i = 0; i < selectColumns.length; i++) {
            SelectColumn selectColumn = selectColumns[i];
            String columnName = "";
            if (selectColumn.getAlias() != null) {
                columnName = selectColumn.getAlias();
            } else if (StringUtils.isNotEmpty(selectColumn.getTableAlias()) && !selectColumn.getSelectColumnName().contains(".")) {
                columnName = selectColumn.getTableAlias() + "." + selectColumn.getSelectColumnName();
            } else {
                columnName = selectColumn.getSelectColumnName();
            }
            maxCNameLen = Math.max(maxCNameLen, columnName.length());
            columnNames[i] = columnName;
            // 最大值长度
            for (Object[] rowValues : resultRows) {
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                maxValueLen = Math.max(maxValueLen, valueStr.length());
            }
        }


        for (int i = 0; i < resultRows.size(); i++) {
            String rowDesc = "[ROW " + (i + 1) + "]";

            int sNum = maxCNameLen - rowDesc.length();
            String rowLine = "-" + rowDesc + "" + repeat("-",  sNum) + "+";
            System.out.println(rowLine + repeat("-",  maxValueLen + 1));

            Object[] row = resultRows.get(i);
            for (int j = 0; j < row.length; j++) {
                String valueStr = (row[j] == null ? "" : valueToString(row[j]));

                // 需要填充的空格数量
                int spaceNum = maxCNameLen - columnNames[j].length();
                System.out.println(columnNames[j] + repeat(" ",  spaceNum) + " | " + valueStr);

                //printRow(new String[]{columnNames[j], valueStr}, new int[]{maxColumnLen, valueStr.length()});
            }
        }
    }





    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    private static String valueToString(Object value) {
        if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.format((Date) value);
        } else {
            return value.toString();
        }
    }



    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }











}
