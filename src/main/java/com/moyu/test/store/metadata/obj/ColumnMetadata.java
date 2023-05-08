package com.moyu.test.store.metadata.obj;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class ColumnMetadata {

    private int totalByteLen;

    private long startPos;

    private int tableId;

    private int columnNameByteLen;

    private int columnNameCharLen;

    private String columnName;

    private byte columnType;

    private int columnLength;


    public ColumnMetadata(int tableId, long startPos, String columnName, byte columnType, int columnLength) {
        this.startPos = startPos;
        this.tableId = tableId;
        this.columnName = columnName;
        this.columnNameByteLen = DataUtils.getDateStringByteLength(columnName);
        this.columnNameCharLen = columnName.length();
        this.columnType = columnType;
        this.columnLength = columnLength;
        this.totalByteLen = 4 + 8 + 4 + 4 + 4 + this.columnNameByteLen + 1 + 4;
    }

    public ColumnMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        this.columnNameByteLen = DataUtils.readInt(byteBuffer);
        this.columnNameCharLen = DataUtils.readInt(byteBuffer);
        this.columnName = DataUtils.readString(byteBuffer, this.columnNameCharLen);
        this.columnType = byteBuffer.get();
        this.columnLength = DataUtils.readInt(byteBuffer);
    }


    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalByteLen);
        DataUtils.writeInt(byteBuffer, totalByteLen);
        DataUtils.writeLong(byteBuffer, startPos);
        DataUtils.writeInt(byteBuffer, tableId);

        DataUtils.writeInt(byteBuffer, columnNameByteLen);
        DataUtils.writeInt(byteBuffer, columnNameCharLen);
        DataUtils.writeStringData(byteBuffer, columnName, columnName.length());

        byteBuffer.put(columnType);
        DataUtils.writeInt(byteBuffer, columnLength);
        byteBuffer.rewind();
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

    public int getColumnNameByteLen() {
        return columnNameByteLen;
    }

    public void setColumnNameByteLen(int columnNameByteLen) {
        this.columnNameByteLen = columnNameByteLen;
    }

    public int getColumnNameCharLen() {
        return columnNameCharLen;
    }

    public void setColumnNameCharLen(int columnNameCharLen) {
        this.columnNameCharLen = columnNameCharLen;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public byte getColumnType() {
        return columnType;
    }

    public void setColumnType(byte columnType) {
        this.columnType = columnType;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", tableId=" + tableId +
                ", columnNameByteLen=" + columnNameByteLen +
                ", columnNameCharLen=" + columnNameCharLen +
                ", columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", columnLength=" + columnLength +
                '}';
    }
}
