package com.moyu.test.store.metadata.obj;

import com.moyu.test.constant.CommonConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class TableMetadata {

    private int totalByteLen;

    private long startPos;

    private int databaseId;

    private int tableId;

    /**
     * 存储引擎类型
     * 0 yuStore
     * 1 yanStore
     */
    private byte engineType;

    private int tableNameByteLen;
    private int tableNameCharLen;
    private String tableName;


    private int createTableSqlByteLen;
    private int createTableSqlCharLen;
    private String createTableSql;

    public TableMetadata(String tableName,
                         int tableId,
                         int databaseId,
                         long startPos,
                         String createTableSql) {
        this.startPos = startPos;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.tableNameByteLen = DataUtils.getDateStringByteLength(tableName);
        this.tableNameCharLen = tableName.length();
        this.createTableSql = createTableSql;
        this.createTableSqlByteLen = DataUtils.getDateStringByteLength(createTableSql);
        this.createTableSqlCharLen = createTableSql.length();
        this.totalByteLen = 4 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 1 +this.tableNameByteLen + this.createTableSqlByteLen;
    }

    public TableMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.databaseId = DataUtils.readInt(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        this.engineType = byteBuffer.get();
        this.tableNameByteLen = DataUtils.readInt(byteBuffer);
        this.tableNameCharLen = DataUtils.readInt(byteBuffer);
        this.tableName = DataUtils.readString(byteBuffer, this.tableNameCharLen);
        this.createTableSqlByteLen = DataUtils.readInt(byteBuffer);
        this.createTableSqlCharLen = DataUtils.readInt(byteBuffer);
        this.createTableSql = DataUtils.readString(byteBuffer, this.createTableSqlCharLen);
    }


    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalByteLen);
        DataUtils.writeInt(byteBuffer, totalByteLen);
        DataUtils.writeLong(byteBuffer, startPos);
        DataUtils.writeInt(byteBuffer, databaseId);
        DataUtils.writeInt(byteBuffer, tableId);
        byteBuffer.put(engineType);

        DataUtils.writeInt(byteBuffer, tableNameByteLen);
        DataUtils.writeInt(byteBuffer, tableNameCharLen);
        DataUtils.writeStringData(byteBuffer, tableName, tableName.length());

        DataUtils.writeInt(byteBuffer, createTableSqlByteLen);
        DataUtils.writeInt(byteBuffer, createTableSqlCharLen);
        DataUtils.writeStringData(byteBuffer, createTableSql, createTableSql.length());
        byteBuffer.rewind();
        return byteBuffer;
    }


    public int getTotalByteLen() {
        return totalByteLen;
    }

    public void setTotalByteLen(int totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public int getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }

    public int getTableNameByteLen() {
        return tableNameByteLen;
    }

    public void setTableNameByteLen(int tableNameByteLen) {
        this.tableNameByteLen = tableNameByteLen;
    }

    public int getTableNameCharLen() {
        return tableNameCharLen;
    }

    public void setTableNameCharLen(int tableNameCharLen) {
        this.tableNameCharLen = tableNameCharLen;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getCreateTableSqlByteLen() {
        return createTableSqlByteLen;
    }

    public void setCreateTableSqlByteLen(int createTableSqlByteLen) {
        this.createTableSqlByteLen = createTableSqlByteLen;
    }

    public int getCreateTableSqlCharLen() {
        return createTableSqlCharLen;
    }

    public void setCreateTableSqlCharLen(int createTableSqlCharLen) {
        this.createTableSqlCharLen = createTableSqlCharLen;
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public void setEngineType(String engineType) {
        byte engineTypeCode = 0;
        if(CommonConstant.ENGINE_TYPE_YAN.equals(engineType)) {
            engineTypeCode = 1;
        }
        this.engineType = engineTypeCode;
    }

    public String getEngineType() {
        if(engineType == (byte)0) {
            return CommonConstant.ENGINE_TYPE_YU;
        } else if(engineType == (byte)1) {
            return CommonConstant.ENGINE_TYPE_YAN;
        } else {
            throw new DbException("不支持存储引擎类型:" + engineType);
        }
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", databaseId=" + databaseId +
                ", tableId=" + tableId +
                ", tableNameByteLen=" + tableNameByteLen +
                ", tableNameCharLen=" + tableNameCharLen +
                ", tableName='" + tableName + '\'' +
                ", createTableSqlByteLen=" + createTableSqlByteLen +
                ", createTableSqlCharLen=" + createTableSqlCharLen +
                ", createTableSql='" + createTableSql + '\'' +
                '}';
    }
}
