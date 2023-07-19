package com.moyu.test.command.dml.expression;

import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;

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
    public Value getValue(LocalSession session) {
        return null;
    }

    @Override
    public Expression optimize() {
        return null;
    }
}
