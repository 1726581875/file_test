package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.common.DynamicByteBuffer;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class TableMeta extends AbstractMeta implements SerializableByte {

    private int totalByteLen;

    private long startPos;
    /**
     * 数据库id
     */
    private int databaseId;
    /**
     * 表id
     */
    private int tableId;

    /**
     * 存储引擎类型
     * 0 yuStore
     * 1 yanStore
     */
    private byte engineType;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表注释
     */
    private String comment;

    public TableMeta(String tableName, int tableId, int databaseId, long startPos, String comment) {
        this.startPos = startPos;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.comment = comment;
    }

    public TableMeta(ByteBuffer byteBuffer) {
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.databaseId = DataByteUtils.readInt(byteBuffer);
        this.tableId = DataByteUtils.readInt(byteBuffer);
        this.engineType = byteBuffer.get();
        this.tableName = readString(byteBuffer);
        this.comment = readString(byteBuffer);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        DynamicByteBuffer byteBuffer = new DynamicByteBuffer();
        byteBuffer.putInt(this.totalByteLen);
        byteBuffer.putLong(this.startPos);
        byteBuffer.putInt(this.databaseId);
        byteBuffer.putInt(this.tableId);
        byteBuffer.put(this.engineType);
        writeString(byteBuffer, this.tableName);
        writeString(byteBuffer, this.comment);
        int totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, totalByteLen);
        this.totalByteLen = totalByteLen;
        return byteBuffer.flipAndGetBuffer();
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
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
                ", engineType=" + engineType +
                ", tableName='" + tableName + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
