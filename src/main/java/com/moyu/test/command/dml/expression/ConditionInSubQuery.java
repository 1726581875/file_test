package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/20
 */
public class ConditionInSubQuery extends Expression {

    /**
     * true：in查询
     * false： not in查询
     */
    private boolean isIn;

    private Expression left;

    private Query subQuery;

    private Cursor subCursor;


    public ConditionInSubQuery(boolean isIn, Expression left, Query subQuery) {
        this.isIn = isIn;
        this.left = left;
        this.subQuery = subQuery;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {

        Object leftValue = left.getValue(rowEntity);

        if (leftValue == null) {
            return false;
        }


        Object v = null;
        if(subCursor == null) {
            subCursor = subQuery.getQueryResultCursor();
        } else {
            subCursor.reset();
        }

        Column column = subQuery.getSelectColumns()[0].getColumn();

        RowEntity rightRow = null;
        while ((rightRow = subCursor.next()) != null) {
            Column rightColumn = rightRow.getColumn(column.getColumnName(), column.getTableAlias());
            if(leftValue.equals(rightColumn.getValue())) {
                v = rightColumn.getValue();
                break;
            }
        }

        if (isIn) {
            return v != null;
        } else {
            return v == null;
        }
    }

    @Override
    public Expression optimize() {
        return null;
    }
}
