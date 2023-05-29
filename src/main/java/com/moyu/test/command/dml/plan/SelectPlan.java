package com.moyu.test.command.dml.plan;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/5/29
 */
public class SelectPlan {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表id
     */
    private Integer tableId;
    /**
     * 是否使用索引
     */
    private boolean useIndex;
    /**
     * 索引字段
     */
    private Column indexColumn;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public boolean isUseIndex() {
        return useIndex;
    }

    public void setUseIndex(boolean useIndex) {
        this.useIndex = useIndex;
    }

    public Column getIndexColumn() {
        return indexColumn;
    }

    public void setIndexColumn(Column indexColumn) {
        this.indexColumn = indexColumn;
    }
}
