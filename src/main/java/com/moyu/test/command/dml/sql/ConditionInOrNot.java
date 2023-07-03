package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionInOrNot extends AbstractCondition {

    private Column column;

    private List<String> values;

    private boolean isIn;

    private boolean inSubQuery;

    private Query subQuery;

    private Cursor subCursor;

    public ConditionInOrNot(Column column, List<String> values, boolean isIn) {
        this.column = column;
        this.values = values;
        this.isIn = isIn;
    }

    public ConditionInOrNot(Column column, Query subQuery, boolean isIn) {
        this.column = column;
        this.subQuery = subQuery;
        this.isIn = isIn;
        this.inSubQuery = true;
    }

    @Override
    public boolean getResult(RowEntity row) {
        if (inSubQuery) {
            return getInSubQueryResult(row);
        } else {
            return getSimpleInResult(row);
        }
    }

    private boolean getSimpleInResult(RowEntity row) {
        Column columnData = getColumnData(column, row);
        Object left = columnData.getValue();

        if (left == null) {
            return false;
        }

        Object v = null;
        for (String value : values) {
            Object valueObj = TypeConvertUtil.convertValueType(value, column.getColumnType());
            if (left.equals(valueObj)) {
                v = valueObj;
                break;
            }
        }

        if (isIn) {
            return v != null;
        } else {
            return v == null;
        }
    }

    private boolean getInSubQueryResult(RowEntity row) {
        Column columnData = getColumnData(column, row);
        Object left = columnData.getValue();

        if (left == null) {
            return false;
        }

        Object v = null;
        if(subCursor == null) {
            subCursor = subQuery.getQueryResultCursor();
        } else {
            subCursor.reset();
        }

        RowEntity rightRow = null;
        while ((rightRow = subCursor.next()) != null) {
            Column rightColumn = getColumnData(column, rightRow);
            if(left.equals(rightColumn.getValue())) {
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
    public void close() {

        if (subQuery != null) {
            subQuery.closeQuery();
        }

        if(subCursor != null) {
            subCursor.close();
            subCursor = null;
        }
    }
}
