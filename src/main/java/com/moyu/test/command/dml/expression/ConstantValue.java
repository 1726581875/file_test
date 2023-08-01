package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Parameter;
import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ConstantValue extends Expression {

    public static final ConstantValue EXPRESSION_TRUE = new ConstantValue(true);

    public static final ConstantValue EXPRESSION_FALSE = new ConstantValue(false);

    private Object value;

    public ConstantValue(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        if (value instanceof Parameter) {
            return ((Parameter) value).getValue();
        }
        return value;
    }

    @Override
    public Expression optimize() {
        return this;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {
        if (value instanceof String) {
            sqlBuilder.append('\'');
            sqlBuilder.append((String) value);
            sqlBuilder.append('\'');
        } else if (value instanceof Parameter) {
            sqlBuilder.append("?" + ((Parameter) value).getIndex());
        } else {
            sqlBuilder.append(String.valueOf(value));
        }
    }


    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof ConstantValue) {
            ConstantValue that = (ConstantValue) o;
            if (value.equals(that.getValue())) {
                return true;
            }
        }
        return false;
    }


    public static ConstantValue getBooleanExpression(boolean value) {
        if(value == true) {
            return EXPRESSION_TRUE;
        }
        return EXPRESSION_FALSE;
    }
}
