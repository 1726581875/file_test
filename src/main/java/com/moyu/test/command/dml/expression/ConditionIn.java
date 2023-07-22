package com.moyu.test.command.dml.expression;

import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/19
 */
public class ConditionIn extends Expression {
    /**
     * true：in查询
     * false： not in查询
     */
    private boolean isIn;

    private Expression left;

    private List<Expression> valueList;

    public ConditionIn(boolean isIn, Expression left, List<Expression> valueList) {
        this.isIn = isIn;
        this.left = left;
        this.valueList = valueList;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {

        Object leftValue = left.getValue(rowEntity);
        if (leftValue == null) {
            return false;
        }

        // 当前行的值是否在valueList里面
        boolean isIncluded = false;
        for (Expression value : valueList) {
            Object v = value.getValue(rowEntity);
            if(leftValue.equals(v)) {
                isIncluded = true;
                break;
            }
        }

        if(isIn) {
            // in
            return isIncluded == true;
        } else {
            // not in
            return isIncluded == false;
        }
    }

    @Override
    public Expression optimize() {
        return this;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }
}
