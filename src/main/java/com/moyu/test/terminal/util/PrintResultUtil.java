package com.moyu.test.terminal.util;

import com.moyu.test.command.QueryResult;
import com.moyu.test.exception.DbException;
import com.moyu.test.net.model.terminal.ColumnMetaDto;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.model.terminal.RowDto;
import com.moyu.test.store.metadata.obj.SelectColumn;
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
     * 横向表格
     * @param queryResult
     */
    public static void printResult(QueryResult queryResult) {
        int columnCount = queryResult.getSelectColumns().length;
        List<Object[]> resultRows = queryResult.getResultRows();
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            SelectColumn selectColumn = queryResult.getSelectColumns()[i];
            String columnName = "";
            if (selectColumn.getAlias() != null) {
                columnName = selectColumn.getAlias();
            } else if (StringUtils.isNotEmpty(selectColumn.getTableAlias()) && !selectColumn.getSelectColumnName().contains(".")) {
                columnName = selectColumn.getTableAlias() + "." + selectColumn.getSelectColumnName();
            } else {
                columnName = selectColumn.getSelectColumnName();
            }
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
            SelectColumn selectColumn = queryResult.getSelectColumns()[i];
            String columnName = "";
            if (selectColumn.getAlias() != null) {
                columnName = selectColumn.getAlias();
            } else if (StringUtils.isNotEmpty(selectColumn.getTableAlias()) && !selectColumn.getSelectColumnName().contains(".")) {
                columnName = selectColumn.getTableAlias() + "." + selectColumn.getSelectColumnName();
            } else {
                columnName = selectColumn.getSelectColumnName();
            }
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

    /**
     * 格式化，打印结果
     * 横向表格
     * @param queryResult
     */
    public static void printResult(QueryResultDto queryResult) {
        if (queryResult == null) {
            //throw new DbException("发生异常，结果为空");
            System.out.println("发生异常，结果为空");
            return;
        }
        ColumnMetaDto[] columns = queryResult.getColumns();

        RowDto[] rows = queryResult.getRows();

        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (RowDto rowValueDto : rows) {
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



    /**
     * 格式化，打印结果
     * 横向表格，只打印前n行结果
     * @param queryResult
     */
    public static void printTopRows(QueryResultDto queryResult, int limit) {
        if (queryResult == null) {
            System.out.println("发生异常，结果为空");
            return;
        }
        ColumnMetaDto[] columns = queryResult.getColumns();
        RowDto[] rows = queryResult.getRows();
        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = getColumnWidths(queryResult);
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
        for (int i = 0; i < limit; i++) {
            Object[] row = rows[i].getColumnValues();
            String[] rowDataStrs = new String[row.length];
            for (int j = 0; j < columnCount; j++) {
                String valueStr = (row[j] == null ? "" : valueToString(row[j]));
                rowDataStrs[j] = valueStr;
            }
            printRow(rowDataStrs, columnWidths);
        }
        printHorizontalLine(columnWidths);
    }

    /**
     * 获取下一行的结果
     * @param queryResult
     * formatType 0横向打印字段,1纵向打印字段
     * @param currRowIndex
     * @return
     */
    public static String  getOutputRowStr(QueryResultDto queryResult, byte formatType, int currRowIndex) {
        if (queryResult == null) {
            return "发生异常，结果为空";
        }
        if (formatType == 0) {
            return getHorizontalRowStr(queryResult, currRowIndex);
        } else if (formatType == 1) {
            return getVerticalRowStr(queryResult, currRowIndex);
        } else {
            throw new DbException("不支持输出格式:" + formatType);
        }
    }


    /**
     * 获取格式化字符串结果
     * @param queryResult
     * @param formatType 0横向打印字段,1纵向打印字段
     * @param limit 输出数据行数，-1表示全部行
     * @return
     */
    public static String getFormatResult(QueryResultDto queryResult, byte formatType, int limit) {
        if (queryResult == null) {
            return "发生异常，结果为空";
        }
        if (formatType == 0) {
            return getHorizontalPrintResult(queryResult, limit);
        } else if (formatType == 1) {
            return getVerticalPrintResult(queryResult, limit);
        } else {
            throw new DbException("不支持输出格式:" + formatType);
        }
    }

    private static String getHorizontalRowStr(QueryResultDto queryResult, int currRowIndex) {
        StringBuilder rowBuilder = new StringBuilder("");
        ColumnMetaDto[] columns = queryResult.getColumns();
        RowDto[] rows = queryResult.getRows();
        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = getColumnWidths(queryResult);

        // 打印当前行
        if(currRowIndex >= rows.length) {
            return "";
        }
        Object[] row = rows[currRowIndex].getColumnValues();
        String[] rowDataStrs = new String[row.length];
        for (int j = 0; j < columnCount; j++) {
            String valueStr = (row[j] == null ? "" : valueToString(row[j]));
            rowDataStrs[j] = valueStr;
        }
        appendRow(rowDataStrs, columnWidths, rowBuilder);
        appendHorizontalLine(columnWidths, rowBuilder);
        return rowBuilder.toString();
    }

    private static String getVerticalRowStr(QueryResultDto queryResult, int idx) {
        StringBuilder result = new StringBuilder("");
        VerticalAttribute att = getVerticalAttribute(queryResult);
        appendVerticalRow(result, queryResult.getRows(), att, idx);
        return result.toString();
    }


    private static String getHorizontalPrintResult(QueryResultDto queryResult, int limit) {
        StringBuilder stringBuilder = new StringBuilder("");
        ColumnMetaDto[] columns = queryResult.getColumns();
        RowDto[] rows = queryResult.getRows();
        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (RowDto rowValueDto : rows) {
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
        limit = limit == -1 ? rows.length : limit;
        // 打印表格内容
        for (int i = 0; i < limit; i++) {
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
        return stringBuilder.toString();
    }

    private static String getVerticalPrintResult(QueryResultDto queryResult, int limit) {
        StringBuilder result = new StringBuilder("");
        RowDto[] resultRows = queryResult.getRows();
        VerticalAttribute att = getVerticalAttribute(queryResult);
        limit = limit == -1 ? resultRows.length : limit;
        for (int i = 0; i < limit; i++) {
            appendVerticalRow(result, resultRows, att, i);
        }
        return result.toString();
    }

    private static VerticalAttribute getVerticalAttribute(QueryResultDto queryResult) {
        ColumnMetaDto[] selectColumns = queryResult.getColumns();
        RowDto[] resultRows = queryResult.getRows();
        String[] columnNames = new String[selectColumns.length];
        // 获取最长字段名长度
        int maxCNameLen = 10;
        // 获取长的值长度
        int maxValueLen = 10;
        for (int i = 0; i < selectColumns.length; i++) {
            String columnName = getPrintColumnName(selectColumns[i]);
            maxCNameLen = Math.max(maxCNameLen, columnName.length());
            columnNames[i] = columnName;
            // 最大值长度
            for (RowDto row : resultRows) {
                Object[] columnValues = row.getColumnValues();
                String valueStr = (columnValues[i] == null ? "" : valueToString(columnValues[i]));
                maxValueLen = Math.max(maxValueLen, valueStr.length());
            }
        }
        return new VerticalAttribute(columnNames, maxCNameLen, maxValueLen);
    }

    private static void appendVerticalRow(StringBuilder result, RowDto[] resultRows,VerticalAttribute att, int idx) {
        String[] columnNames = att.getColumnNames();
        String rowDesc = "[ROW " + (idx + 1) + "]";
        String rowLine = "-" + rowDesc  + repeat("-",  att.getMaxCNameLen() - rowDesc.length()) + "+";
        result.append(rowLine + repeat("-",  att.getMaxValueLen() + 1));
        result.append("\n");
        Object[] row = resultRows[idx].getColumnValues();
        for (int j = 0; j < columnNames.length; j++) {
            String valueStr = (row[j] == null ? "" : valueToString(row[j]));
            // 需要填充的空格数量
            int spaceNum = att.getMaxCNameLen() - columnNames[j].length();
            result.append(columnNames[j] + repeat(" ",  spaceNum) + " | " + valueStr);
            result.append("\n");
        }
    }

    static class VerticalAttribute {
        String[] columnNames;
        int maxCNameLen;
        int maxValueLen;

        public VerticalAttribute(String[] columnNames, int maxCNameLen, int maxValueLen) {
            this.columnNames = columnNames;
            this.maxCNameLen = maxCNameLen;
            this.maxValueLen = maxValueLen;
        }


        public String[] getColumnNames() {
            return columnNames;
        }

        public int getMaxCNameLen() {
            return maxCNameLen;
        }

        public int getMaxValueLen() {
            return maxValueLen;
        }
    }



    private static int[] getColumnWidths(QueryResultDto queryResult) {
        ColumnMetaDto[] columns = queryResult.getColumns();
        RowDto[] rows = queryResult.getRows();
        int columnCount = columns.length;
        // 计算字段宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = getPrintColumnName(columns[i]);
            // 可设置最小宽度
            columnWidths[i] = Math.max(columnName.length(), 10);
            // 遍历结果集的所有行，计算该列数据的宽度
            for (RowDto rowValueDto : rows) {
                Object[] rowValues = rowValueDto.getColumnValues();
                String valueStr = (rowValues[i] == null ? "" : valueToString(rowValues[i]));
                columnWidths[i] = Math.max(columnWidths[i], valueStr.length());
            }
        }
        return columnWidths;
    }



    private static String getPrintColumnName(ColumnMetaDto columnDto) {
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
            System.out.print("| ");
            // 输出值
            System.out.print(value);
            // 需要填充的空格数量
            int spaceNum = width - value.length();
            // 打印对应数量的空格
            System.out.printf(repeat(" ",  spaceNum) + " ");
        }
        System.out.println("|");
    }

    public static void printHorizontalLine(int[] columnWidths) {
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
