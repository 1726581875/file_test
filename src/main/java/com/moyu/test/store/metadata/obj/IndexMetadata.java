package com.moyu.test.store.metadata.obj;

import com.moyu.test.store.SerializableByte;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class IndexMetadata implements SerializableByte {

    private int totalByteLen;

    private long startPos;

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
     * 是否主键索引
     */
    private byte isPrimaryKey;


    public IndexMetadata(long startPos, int tableId, String indexName, String columnName, byte isPrimaryKey) {
        this.startPos = startPos;
        this.tableId = tableId;
        this.indexName = indexName;
        this.columnName = columnName;
        this.isPrimaryKey = isPrimaryKey;
        this.totalByteLen = 4 + 8 + 4 +  1 + (this.indexName.length() * 3) + (this.columnName.length() * 3) + 2;
    }

    public IndexMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        int l1 = DataUtils.readInt(byteBuffer);
        this.indexName = DataUtils.readString(byteBuffer, l1);
        int l2 = DataUtils.readInt(byteBuffer);
        this.columnName = DataUtils.readString(byteBuffer, l2);
        this.isPrimaryKey = byteBuffer.get();
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.totalByteLen);
        byteBuffer.putInt(this.totalByteLen);
        byteBuffer.putLong(this.startPos);
        byteBuffer.putInt(this.tableId);
        byteBuffer.putInt(this.indexName.length());
        DataUtils.writeStringData(byteBuffer, this.indexName, this.indexName.length());
        byteBuffer.putInt(this.columnName.length());
        DataUtils.writeStringData(byteBuffer, this.columnName, this.columnName.length());
        byteBuffer.put(this.isPrimaryKey);
        // 获取真实长度
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, this.totalByteLen);
        byteBuffer.flip();
        return byteBuffer;
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

    public byte getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(byte isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }


    @Override
    public String toString() {
        return "IndexMetadata{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", tableId=" + tableId +
                ", indexName='" + indexName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", isPrimaryKey=" + isPrimaryKey +
                '}';
    }
}
