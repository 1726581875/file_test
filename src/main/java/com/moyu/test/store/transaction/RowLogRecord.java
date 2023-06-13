package com.moyu.test.store.transaction;

import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class RowLogRecord {
    /**
     * 事务id
     */
    private int transactionId;

    /**
     * 数据库id
     */
    private Integer databaseId;
    /**
     * 所属表名
     */
    private String tableName;
    /**
     * 锁住数据块位置
     */
    private long blockPos;
    /**
     * 数据行id
     */
    private long rowId;
    /**
     * 版本
     */
    private int version;
    /**
     * 旧数据
     */
    private RowData oldRow;

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getBlockPos() {
        return blockPos;
    }

    public void setBlockPos(long blockPos) {
        this.blockPos = blockPos;
    }

    public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public RowData getOldRow() {
        return oldRow;
    }

    public void setOldRow(RowData oldRow) {
        this.oldRow = oldRow;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }
}
