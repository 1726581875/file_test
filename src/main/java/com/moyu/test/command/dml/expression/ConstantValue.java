package com.moyu.test.command.dml.expression;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ConstantValue extends Expression {

    private Object value;

    public ConstantValue(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        return value;
    }

    @Override
    public Expression optimize() {
        return null;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {
        if(value instanceof String) {
            sqlBuilder.append('\'');
            sqlBuilder.append((String)value);
            sqlBuilder.append('\'');
        } else {
            sqlBuilder.append(String.valueOf(value));
        }
    }


    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        ConstantValue that = (ConstantValue) o;
        if(value.equals(that.getValue())) {
            return true;
        }
        return false;
    }
}
