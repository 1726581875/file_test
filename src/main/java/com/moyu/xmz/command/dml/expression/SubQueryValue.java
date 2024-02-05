package com.moyu.xmz.command.dml.expression;

import com.moyu.xmz.command.dml.sql.Query;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.store.cursor.Cursor;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.common.util.TypeConvertUtils;

/**
 * @author xiaomingzhang
 * @date 2023/7/20
 */
public class SubQueryValue extends Expression {

    private ConditionColumnExpr leftColumn;
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

    public SubQueryValue(ConditionColumnExpr leftColumn, Query subQuery) {
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
            subQueryValue = TypeConvertUtils.convertValueType(String.valueOf(subQueryValue), leftColumn.getColumn().getColumnType());
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
