package com.moyu.test.command.dml.function;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public abstract class StatFunction implements DbFunction {

    protected String columnName;

    protected Long value;

    public StatFunction(String columnName, Long value) {
        this.columnName = columnName;
        this.value = value;
    }

    public abstract void stat(Column[] columns);


    public Long getValue() {
        return value;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
