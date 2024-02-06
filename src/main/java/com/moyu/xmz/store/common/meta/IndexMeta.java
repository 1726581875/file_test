package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class IndexMeta extends AbstractMeta implements SerializableByte {

    private int totalByteLen;

    private long startPos;
    /**
     * 表id
     */
    private int tableId;
    /**
     * 索引名
     */
    private String indexName;
    /**
     * 字段名
     */
    private String columnName;
    /**
     * 索引类型，1主键索引、2普通索引
     */
    private byte indexType;



    public IndexMeta(long startPos, int tableId, String indexName, String columnName, byte indexType) {
        this.startPos = startPos;
        this.tableId = tableId;
        this.indexName = indexName;
        this.columnName = columnName;
        this.indexType = indexType;
    }

    public IndexMeta(ByteBuffer byteBuffer) {
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.tableId = DataByteUtils.readInt(byteBuffer);
        this.indexName = readString(byteBuffer);
        this.columnName = readString(byteBuffer);
        this.indexType = byteBuffer.get();
    }


    @Override
    public ByteBuffer getByteBuffer() {
        DynByteBuffer byteBuffer = new DynByteBuffer();
        byteBuffer.putInt(this.totalByteLen);
        byteBuffer.putLong(this.startPos);
        byteBuffer.putInt(this.tableId);
        writeString(byteBuffer, this.indexName);
        writeString(byteBuffer, this.columnName);
        byteBuffer.put(this.indexType);
        // 获取字节长度
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, this.totalByteLen);
        return byteBuffer.flipAndGetBuffer();
    }

    public int getTotalByteLen() {
        return totalByteLen;
    }

    public void setTotalByteLen(int totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }


    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }


    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public byte getIndexType() {
        return indexType;
    }

    public void setIndexType(byte indexType) {
        this.indexType = indexType;
    }


    @Override
    public String toString() {
        return "IndexMetadata{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", tableId=" + tableId +
                ", indexName='" + indexName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", isPrimaryKey=" + indexType +
                '}';
    }
}
