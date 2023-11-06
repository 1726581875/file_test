package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.config.CommonConfig;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.cursor.*;
import com.moyu.test.store.metadata.obj.SelectColumn;
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

    private int resultRowNum;


    public SelectCommand(Query query) {
        this.query = query;
    }

    @Override
    public QueryResult execCommand() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        Cursor queryResultCursor = this.query.getQueryResultCursor();
        queryResult = parseQueryResult(queryResultCursor);

        long queryEndTime = System.currentTimeMillis();
        String desc = "查询结果行数:" + queryResult.getResultRows().size() + ", 耗时:" + (queryEndTime - queryStartTime) + "ms";
        queryResult.setDesc(desc);
        return queryResult;
    }


    /**
     * @return
     */
    public QueryResult getNextPageResult() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        Cursor queryResultCursor = this.query.getQueryResultCursor();
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());

        byte hasNext = 0;
        int rowNum = 0;
        try {
            RowEntity row = null;
            while ((row = queryResultCursor.next()) != null) {
                rowNum++;
                resultRowNum++;
                result.addRow(row.getColumns());
                if (rowNum >= CommonConfig.NET_TRAN_MAX_ROW_SIZE) {
                    hasNext = 1;
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            queryResultCursor.close();
            this.query.closeQuery();
            throw new DbException("查询发生异常");
        }
        long queryEndTime = System.currentTimeMillis();
        if (hasNext == (byte) 0) {
            String desc = "查询结果行数:" + resultRowNum + ", 耗时:" + (queryEndTime - queryStartTime) + "ms";
            result.setDesc(desc);
            queryResultCursor.close();
            this.query.closeQuery();
        }
        result.setHasNext(hasNext);
        return result;
    }

    public Cursor getResultCursor() {
        return this.query.getQueryResultCursor();
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


    @Override
    public void reUse() {
        query.resetQueryCursor();
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }
}
