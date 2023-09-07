package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.cursor.*;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private Query query;
    /**
     * 查询结果
     */
    private QueryResult queryResult;


    public SelectCommand(Query query) {
        this.query = query;
    }

    @Override
    public String execute() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        Cursor queryResultCursor = this.query.getQueryResultCursor();

        queryResult = parseQueryResult(queryResultCursor);
        long queryEndTime = System.currentTimeMillis();

        // 解析结果，打印拼接结果字符串
        return getResultPrintStr(queryResult, queryStartTime, queryEndTime);
    }


    @Override
    public QueryResult execCommand() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        Cursor queryResultCursor = this.query.getQueryResultCursor();
        queryResult = parseQueryResult(queryResultCursor);

        long queryEndTime = System.currentTimeMillis();
        String desc = "查询结果行数:" +  queryResult.getResultRows().size() + ", 耗时:" + (queryEndTime - queryStartTime)  + "ms";
        queryResult.setDesc(desc);
        return queryResult;
    }

    private QueryResult parseQueryResult(Cursor cursor) {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        try {
            RowEntity row = null;
            while ((row = cursor.next()) != null) {
                result.addRow(row.getColumns());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("查询发生异常");
        } finally {
            cursor.close();
            this.query.closeQuery();
        }
        return result;
    }



    private void appendLine(StringBuilder stringBuilder) {
        stringBuilder.append(" ");
        for (SelectColumn column : query.getSelectColumns()) {
            int length = column.getSelectColumnName().length();
            while (length > 0) {
                stringBuilder.append("--");
                length--;
            }
        }
        stringBuilder.append("\n");
    }

    private String getStr(char c, int num) {
        StringBuilder stringBuilder = new StringBuilder();
        while (num > 0) {
            stringBuilder.append(c);
            num--;
        }
        return stringBuilder.toString();
    }


    private String getResultPrintStr(QueryResult queryResult, long queryStartTime,long queryEndTime) {
        // 解析结果，打印拼接结果字符串
        StringBuilder stringBuilder = new StringBuilder();
        // 分界线
        appendLine(stringBuilder);
        // 表头
        String tableHeaderStr = "";
        for (SelectColumn column : query.getSelectColumns()) {
            String value = getColumnNameStr(column);
            tableHeaderStr = tableHeaderStr + " | " + value;
        }
        stringBuilder.append(tableHeaderStr + " | " + "\n");

        // 分界线
        appendLine(stringBuilder);

        SelectColumn[] resultColumns = queryResult.getSelectColumns();
        // 值
        List<Object[]> resultRows = queryResult.getResultRows();
        for (int i = 0; i < resultRows.size(); i++) {
            Object[] rowValues = resultRows.get(i);
            String rowStr = "";
            for (int j = 0; j < rowValues.length; j++) {
                Object value = rowValues[j];
                String valueStr = (value == null ? "" : valueToString(value));
                int length = getColumnNameStr(resultColumns[j]).length();
                if(length > valueStr.length()) {
                    int spaceNum = (length - valueStr.length()) / 2;
                    rowStr = rowStr + " | "+ getStr(' ',spaceNum) + valueStr + getStr(' ',spaceNum);
                } else {
                    rowStr = rowStr + " | " + valueStr;
                }
            }
            stringBuilder.append(rowStr + " | " + "\n");
        }

        stringBuilder.append("查询结果行数:" +  resultRows.size() + ", 耗时:" + (queryEndTime - queryStartTime)  + "ms");

        return stringBuilder.toString();
    }


    private String getColumnNameStr(SelectColumn column) {
        String value = column.getAlias() == null ? column.getSelectColumnName() : column.getAlias();
        if (column.getTableAlias() != null) {
            value = column.getTableAlias() + "." + value;
        }
        return value;
    }

    private String valueToString(Object value) {
        if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.format((Date) value);
        } else {
            return value.toString();
        }
    }

    @Override
    public void reUse() {
       query.resetQueryCursor();
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }
}
