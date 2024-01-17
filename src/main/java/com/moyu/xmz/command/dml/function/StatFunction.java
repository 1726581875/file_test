package com.moyu.xmz.command.dml.function;

import com.moyu.xmz.store.common.dto.Column;

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
