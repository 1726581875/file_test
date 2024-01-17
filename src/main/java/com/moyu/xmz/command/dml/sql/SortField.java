package com.moyu.xmz.command.dml.sql;

import com.moyu.xmz.store.common.dto.Column;

/**
 * @author xiaomingzhang
 * @date 2023/8/22
 */
public class SortField {

    public static final String RULE_ASC = "ASC";
    public static final String RULE_DESC = "DESC";

    public static final String DEFAULT_RULE = RULE_ASC;

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
