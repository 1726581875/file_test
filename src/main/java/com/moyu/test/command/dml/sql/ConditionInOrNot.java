package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionInOrNot extends AbstractCondition2 {

    private Column column;

    private List<String> values;

    private boolean isIn;

    public ConditionInOrNot(Column column, List<String> values, boolean isIn) {
        this.column = column;
        this.values = values;
        this.isIn = isIn;
    }

    @Override
    public boolean getResult(RowEntity row) {
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
            }
        }

        if (isIn) {
            return v != null;
        } else {
            return v == null;
        }
    }
}
