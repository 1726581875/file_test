package test;

import com.moyu.test.command.Command;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.DropDatabaseCommand;
import com.moyu.test.command.dml.SelectCommand;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.terminal.util.PrintResultUtil;

import java.text.SimpleDateFormat;
import java.util.Arrays;
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
        DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand(databaseName, true);
        dropDatabaseCommand.execute();
        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(databaseName);
        createDatabaseCommand.execute();
        database = Database.getDatabase(databaseName);
    }


    public static void main(String[] args) {
        String sql = "show databases";
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        PrintResultUtil.printResult(queryResult);
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
        SelectCommand command = (SelectCommand) connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);



        System.out.println();


        int columnCount = queryResult.getSelectColumns().length;
        List<Object[]> resultRows = queryResult.getResultRows();

        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Column column = queryResult.getSelectColumns()[i].getColumn();
            String columnName = column.getColumnName();
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);

            // 遍历结果集的所有行，计算该列数据的宽度
            for (Object[] rowValues : resultRows ) {
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
        }

        // 打印表格顶部边框
        printHorizontalLine(columnWidths);
        // 打印表头
        String[] tableHeaders = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Column column = queryResult.getSelectColumns()[i].getColumn();
            String columnName = column.getColumnName();
            tableHeaders[i] = columnName;
        }
        printRow(tableHeaders, columnWidths);
        // 打印表头与内容之间的分隔线
        printHorizontalLine(columnWidths);
        // 打印表格内容
        for (int i = 0; i < resultRows.size(); i++) {
            Object[] row = resultRows.get(i);
            String[] rowData = new String[row.length];
            for (int j = 0; j < columnCount; j++) {
                String valueStr = (row[j] == null ? "" : valueToString(row[j]));
                rowData[j] = valueStr;
            }
            printRow(rowData, columnWidths);
        }
        // 打印表格底部边框
        printHorizontalLine(columnWidths);
    }



    private static void printRow(String[] rowData, int[] columnWidths) {
        for (int i = 0; i < rowData.length; i++) {
            System.out.printf("| %-" + columnWidths[i] + "s ", rowData[i]);
        }
        System.out.println("|");
    }



    private static void printHorizontalLine(int[] columnWidths) {
        for (int width : columnWidths) {
            System.out.print("+");
            for (int i = 0; i < width + 2; i++) {
                System.out.print("-");
            }
        }
        System.out.println("+");
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
