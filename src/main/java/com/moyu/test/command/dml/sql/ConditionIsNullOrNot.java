package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/11
 */
public class ConditionIsNullOrNot extends AbstractCondition {

    private Column column;

    private boolean isNull;


    public ConditionIsNullOrNot(Column column, boolean isNull) {
        this.column = column;
        this.isNull = isNull;
    }

    @Override
    public boolean getResult(RowEntity row) {
        Object columnValue = getColumnData(column, row).getValue();
        return isNull ? columnValue == null : columnValue != null;
    }
}
