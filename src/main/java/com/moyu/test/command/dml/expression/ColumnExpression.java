package com.moyu.test.command.dml.expression;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ColumnExpression extends Expression {

    private Column column;

    public ColumnExpression(Column column) {
        this.column = column;
    }
}
