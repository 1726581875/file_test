package com.moyu.xmz.command.dml.expression;

import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public abstract class AbstractCondition extends Expression {

    @Override
    public Object getValue(RowEntity rowEntity) {
        return null;
    }
    @Override
    public Expression optimize() {
        return null;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }
}
