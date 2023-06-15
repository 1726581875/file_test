package com.moyu.test.store.buffer;

import com.moyu.test.store.data.DataChunk;

/**
 * @author xiaomingzhang
 * @date 2023/6/15
 */
public class BufferDataChunk {

    /**
     * 缓存未修改，此时缓存数据和磁盘磁盘一致
     */
    public static final int NOT_MODIFIED = 0;
    /**
     * 缓存已发生修改但是未提交，此时缓存数据和磁盘数据不一致
     */
    public static final int MODIFIED_NOT_COMMIT = 1;
    /**
     * 缓存数据更新到磁盘进行中，此时缓存数据和磁盘数据不一致
     */
    public static final int SUBMITTING = 2;
    /**
     * 缓存数据已提交到磁盘，此时缓存数据和磁盘数据一致
     */
    public static final int COMMITTED = 4;

    private Integer databaseId;

    private String tableName;

    /**
     * 缓存状态
     */
    private int status;
    /**
     * 缓存数据块
     */
    private DataChunk dataChunk;

    public BufferDataChunk(Integer databaseId, String tableName, int status, DataChunk dataChunk) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.status = status;
        this.dataChunk = dataChunk;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public DataChunk getDataChunk() {
        return dataChunk;
    }

    public void setDataChunk(DataChunk dataChunk) {
        this.dataChunk = dataChunk;
    }
}
