package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 * 表示条件等于或者不等于
 * 如: a = 1 、a != 1
 */
public class ConditionEqOrNq extends AbstractCondition2 {

    private Column column;

    private String value;
    /**
     * true等于
     * false 不等于
     */
    private boolean isEq;


    public ConditionEqOrNq(Column column, String value, boolean isEq) {
        this.column = column;
        this.value = value;
        this.isEq = isEq;
    }

    @Override
    public boolean getResult(RowEntity row) {
        Object columnValue = getColumnData(column, row).getValue();
        Object rightValue = TypeConvertUtil.convertValueType(value, column.getColumnType());

        if (columnValue == null) {
            return false;
        }

        if (isEq) {
            return columnValue.equals(rightValue);
        } else {
            return !columnValue.equals(rightValue);
        }
    }

    public Column getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    public boolean isEq() {
        return isEq;
    }
}
