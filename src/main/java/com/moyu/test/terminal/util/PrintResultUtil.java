package com.moyu.test.terminal.util;

import com.moyu.test.command.QueryResult;
import com.moyu.test.exception.DbException;
import com.moyu.test.net.model.terminal.ColumnDto;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.model.terminal.RowValueDto;
import com.moyu.test.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/6
 */
public class PrintResultUtil {

    /**
     * 格式化，打印结果
     * @param queryResult
     */
    public static void printResult(QueryResult queryResult) {
        int columnCount = queryResult.getSelectColumns().length;
        List<Object[]> resultRows = queryResult.getResultRows();
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = queryResult.getSelectColumns()[i].getSelectColumnName();
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (Object[] rowValues : resultRows) {
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
        }

        // 打印表格顶部边框
        printHorizontalLine(columnWidths);
        // 打印表头
        String[] tableHeaders = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = queryResult.getSelectColumns()[i].getSelectColumnName();
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

        if(queryResult.getDesc() != null) {
            System.out.println(queryResult.getDesc());
        }
        System.out.println();
    }


    public static void printResult(QueryResultDto queryResult) {
        if (queryResult == null) {
            //throw new DbException("发生异常，结果为空");
            System.out.println("发生异常，结果为空");
            return;
        }
        ColumnDto[] columns = queryResult.getColumns();

        RowValueDto[] rows = queryResult.getRows();

        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (RowValueDto rowValueDto : rows) {
                Object[] rowValues = rowValueDto.getColumnValues();
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
        }

        // 打印表格顶部边框
        printHorizontalLine(columnWidths);
        // 打印表头
        String[] tableHeaders = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            tableHeaders[i] = columnName;
        }
        printRow(tableHeaders, columnWidths);
        // 打印表头与内容之间的分隔线
        printHorizontalLine(columnWidths);
        // 打印表格内容
        for (int i = 0; i < rows.length; i++) {
            Object[] row = rows[i].getColumnValues();
            String[] rowDataStrs = new String[row.length];
            for (int j = 0; j < columnCount; j++) {
                String valueStr = (row[j] == null ? "" : valueToString(row[j]));
                rowDataStrs[j] = valueStr;
            }
            printRow(rowDataStrs, columnWidths);
        }
        // 打印表格底部边框
        printHorizontalLine(columnWidths);

        if(queryResult.getDesc() != null) {
            System.out.println(queryResult.getDesc());
        }
        System.out.println();
    }

    private static String getPrintColumnName(ColumnDto columnDto) {
        if(columnDto.getAlias() != null) {
            return columnDto.getAlias();
        }
        if(StringUtils.isNotEmpty(columnDto.getTableAlias()) && !columnDto.getColumnName().contains(".")) {
            return columnDto.getTableAlias() + "." + columnDto.getColumnName();
        }
        return columnDto.getColumnName();
    }


    private static void printRow(String[] rowData, int[] columnWidths) {
        for (int i = 0; i < rowData.length; i++) {
            int width = columnWidths[i];
            String value = rowData[i];
            System.out.printf("| ");
            // 输出值
            System.out.printf(value);
            // 需要填充的空格数量
            int spaceNum = width - value.length();
            // 打印对应数量的空格
            System.out.printf(repeat(" ",  spaceNum) + " ");
        }
        System.out.println("|");
    }

    private static void printHorizontalLine(int[] columnWidths) {
        for (int width : columnWidths) {
            String line = "+" + repeat("-", width + 2);
            System.out.print(line);
        }
        System.out.println("+");
    }

    /**
     * 重复字符串指定次数。
     */
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


    public static String getFormatResult(QueryResultDto queryResult) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (queryResult == null) {
            stringBuilder.append("发生异常，结果为空");
            return stringBuilder.toString();
        }

        ColumnDto[] columns = queryResult.getColumns();
        RowValueDto[] rows = queryResult.getRows();

        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (RowValueDto rowValueDto : rows) {
                Object[] rowValues = rowValueDto.getColumnValues();
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
        }

        // 打印表格顶部边框
        appendHorizontalLine(columnWidths, stringBuilder);
        // 打印表头
        String[] tableHeaders = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            tableHeaders[i] = columnName;
        }
        appendRow(tableHeaders, columnWidths, stringBuilder);
        // 打印表头与内容之间的分隔线
        appendHorizontalLine(columnWidths, stringBuilder);
        // 打印表格内容
        for (int i = 0; i < rows.length; i++) {
            Object[] row = rows[i].getColumnValues();
            String[] rowDataStrs = new String[row.length];
            for (int j = 0; j < columnCount; j++) {
                String valueStr = (row[j] == null ? "" : valueToString(row[j]));
                rowDataStrs[j] = valueStr;
            }
            appendRow(rowDataStrs, columnWidths, stringBuilder);
        }
        // 打印表格底部边框
        appendHorizontalLine(columnWidths, stringBuilder);

        if(queryResult.getDesc() != null) {
            stringBuilder.append(queryResult.getDesc() + "\n");
        }
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }


    private static void appendHorizontalLine(int[] columnWidths, StringBuilder stringBuilder) {
        for (int width : columnWidths) {
            String line = "+" + repeat("-", width + 2);
            stringBuilder.append(line);
        }
        stringBuilder.append("+\n");
    }


    private static void appendRow(String[] rowData, int[] columnWidths, StringBuilder stringBuilder) {
        for (int i = 0; i < rowData.length; i++) {
            int width = columnWidths[i];
            String value = rowData[i];
            stringBuilder.append("| ");
            // 输出值
            stringBuilder.append(value);
            // 需要填充的空格数量
            int spaceNum = width - value.length();
            // 打印对应数量的空格
            stringBuilder.append(repeat(" ",  spaceNum) + " ");
        }
        stringBuilder.append("|\n");
    }


}
