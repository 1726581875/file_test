package com.moyu.test.command.dml.sql;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/8/22
 */
public class OrderField {
    /**
     * order by 字段
     */
    private Column column;

    /**
     * 排序方式 ASC/DESC
     */
    private String type;


    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
