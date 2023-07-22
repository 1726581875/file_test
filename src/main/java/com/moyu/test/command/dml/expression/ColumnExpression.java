package com.moyu.test.command.dml.expression;

import com.moyu.test.store.data.cursor.RowEntity;
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
    public Expression optimize() {
        return this;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {
        sqlBuilder.append(column.getTableAliasColumnName());
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object o) {
        ColumnExpression that = (ColumnExpression) o;
        if(column == that.getColumn()) {
            return true;
        }
        if(column.getColumnName().equals(that.getColumn().getColumnName())) {
            return true;
        }
        return false;
    }

}
