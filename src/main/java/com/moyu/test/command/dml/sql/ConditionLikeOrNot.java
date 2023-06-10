package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;
import com.sun.deploy.util.StringUtils;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionLikeOrNot extends AbstractCondition2 {

    private Column column;

    private String value;

    private boolean isLike;


    public ConditionLikeOrNot(Column column, String value, boolean isLike) {
        this.column = column;
        this.value = value;
        this.isLike = isLike;
    }

    @Override
    public boolean getResult(RowEntity row) {
        Column columnData = getColumnData(column, row);
        Object left = columnData.getValue();

        if(left == null) {
            return false;
        }
        Object right = TypeConvertUtil.convertValueType(value, column.getColumnType());
        // TODO like操作不能简单contains
        if(isLike) {
            return ((String) left).contains((String) right);
        } else {
            return !((String) left).contains((String) right);
        }
    }
}
