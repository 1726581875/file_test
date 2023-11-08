package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.expression.Expression;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.config.CommonConfig;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.cursor.*;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.CollectionUtils;

import java.io.IOException;
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
    public QueryResult getNextPageResult() throws IOException {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询

        boolean isSimpleQuery = false;

        Cursor queryResultCursor = null;
        // 优化简单查询例如: select * from table。数据量大的时候如果经过getQueryResultCursor()方法遍历了多次还会物化导致会特别慢
        if (    // 非join语句
                this.query.getMainTable() != null && CollectionUtils.isEmpty(this.query.getMainTable().getJoinTables())
                        // 非函数查询
                        && !this.query.isUseFunction()
                        // 非group by语句
                        && CollectionUtils.isEmpty(this.query.getGroupFields())
                        // 非order by语句
                        && CollectionUtils.isEmpty(this.query.getSortFields())
                        // 非子查询
                        && (this.query.getMainTable() != null && this.query.getMainTable().getSubQuery() == null)) {
            isSimpleQuery = true;
            queryResultCursor = this.query.getQueryCursor(this.query.getMainTable());
        } else {
            queryResultCursor = this.query.getQueryResultCursor();
        }

        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());

        byte hasNext = 0;
        int rowNum = 0;
        int currIndex = 0;
        try {
            RowEntity row = null;
            while ((row = queryResultCursor.next()) != null) {
                rowNum++;
                resultRowNum++;
                if(isSimpleQuery) {
                    boolean matchCondition = Expression.isMatch(row, query.getCondition());
                    // 只拿符合条件的行
                    // 使用order by后，offset limit条件就先不筛选。等后面排完序再筛选行
                    if (matchCondition &&  query.isMatchLimit(query, currIndex)) {
                        Column[] resultColumns = query.filterColumns(row.getColumns(), query.getSelectColumns());
                        result.addRow(resultColumns);
                    }
                    if ((query.getLimit() != null && result.getResultRows().size() >= query.getLimit())) {
                        break;
                    }
                    currIndex++;
                } else {
                    result.addRow(row.getColumns());
                }
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

    @Override
    public void reUse() {
        query.resetQueryCursor();
    }

    public QueryResult getQueryResult() {
        return queryResult;
    }
}
