package com.moyu.test.command.dml.expression;

import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;
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

    @Override
    public Object getValue(RowEntity rowEntity) {
        Column column = rowEntity.getColumn(this.column.getColumnName(), this.column.getTableAlias());
        return column.getValue();
    }

    @Override
    public Value getValue(LocalSession session) {

        return null;
    }

    @Override
    public Expression optimize() {
        return null;
    }

    public Column getColumn() {
        return column;
    }
}
