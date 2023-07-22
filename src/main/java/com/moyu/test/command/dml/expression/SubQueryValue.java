package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;
import com.moyu.test.util.TypeConvertUtil;

/**
 * @author xiaomingzhang
 * @date 2023/7/20
 */
public class SubQueryValue extends Expression {

    private ColumnExpression leftColumn;
    /**
     * 子查询对象
     */
    private Query subQuery;
    /**
     * 子查询游标
     */
    private Cursor subQueryResultCursor;
    /**
     * 子查询最终值
     */
    private Object subQueryValue;

    public SubQueryValue(ColumnExpression leftColumn, Query subQuery) {
        this.leftColumn = leftColumn;
        this.subQuery = subQuery;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        subQueryResultCursor = subQuery.getQueryResultCursor();
        RowEntity rightRow = subQueryResultCursor.next();
        subQueryValue = rightRow == null ? null : rightRow.getColumns()[0].getValue();
        if (subQueryResultCursor.next() != null) {
            throw new SqlExecutionException("等于条件判断异常，子查询有多个值");
        }
        if (subQueryValue != null) {
            subQueryValue = TypeConvertUtil.convertValueType(String.valueOf(subQueryValue), leftColumn.getColumn().getColumnType());
        }
        return subQueryValue;
    }
    @Override
    public Expression optimize() {
        return this;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }
}
