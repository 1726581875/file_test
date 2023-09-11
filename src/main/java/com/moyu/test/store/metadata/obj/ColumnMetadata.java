package com.moyu.test.store.metadata.obj;

import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class ColumnMetadata {

    /**
     * 所有字段占用字节(包含自身)
     */
    private int totalByteLen;
    /**
     * 所属文件中开始位置
     */
    private long startPos;
    /**
     * 列字段所属表id
     */
    private int tableId;
    /**
     * 字段名字节长度
     */
    private int columnNameByteLen;
    /**
     * 字段名字符长度
     */
    private int columnNameCharLen;
    /**
     * 字段名
     */
    private String columnName;
    /**
     * 字段类型，
     * @see ColumnTypeConstant
     */
    private byte columnType;
    /**
     * 字段下标
     */
    private int columnIndex;
    /**
     * 字段长度
     */
    private int columnLength;
    /**
     * 是否主键索引 1:是 、0否
     */
    private byte isPrimaryKey;


    public ColumnMetadata(int tableId,
                          long startPos,
                          String columnName,
                          byte columnType,
                          int columnIndex,
                          int columnLength) {
        this.startPos = startPos;
        this.tableId = tableId;
        this.columnName = columnName;
        this.columnNameByteLen = DataUtils.getDateStringByteLength(columnName);
        this.columnNameCharLen = columnName.length();
        this.columnType = columnType;
        this.columnIndex = columnIndex;
        this.columnLength = columnLength;
        this.totalByteLen = 4 + 8 + 4 + 4 + 4 + this.columnNameByteLen + 1 + 4 + 4 + 1;
        this.isPrimaryKey = 0;
    }

    public ColumnMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        this.columnNameByteLen = DataUtils.readInt(byteBuffer);
        this.columnNameCharLen = DataUtils.readInt(byteBuffer);
        this.columnName = DataUtils.readString(byteBuffer, this.columnNameCharLen);
        this.columnType = byteBuffer.get();
        this.columnIndex = DataUtils.readInt(byteBuffer);
        this.columnLength = DataUtils.readInt(byteBuffer);
        this.isPrimaryKey = byteBuffer.get();
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
        DataUtils.writeInt(byteBuffer, columnIndex);
        DataUtils.writeInt(byteBuffer, columnLength);
        byteBuffer.put(isPrimaryKey);
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

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }


    public byte getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(byte isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }


}
