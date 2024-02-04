package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.common.DynamicByteBuffer;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.store.common.dto.Column;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class ColumnMetadata extends AbstractMetadata implements SerializableByte {
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
    /**
     * 字段注释，允许空的字段
     */
    private String comment;
    /**
     * 是否允许为空 1是、0否
     */
    private byte isNotNull;
    /**
     * 字段默认值,如果是空或者NULL表示默认空
     * 如果有默认值，字符传、日期这类会带上单引号或者双引号
     */
    private String defaultVal;


    public ColumnMetadata(int tableId, long startPos, Column columnDto) {
        this.startPos = startPos;
        this.tableId = tableId;
        this.columnName = columnDto.getColumnName();
        this.columnType = columnDto.getColumnType();
        this.columnIndex = columnDto.getColumnIndex();
        this.columnLength = columnDto.getColumnLength();
        this.isPrimaryKey = columnDto.getIsPrimaryKey();
        this.isNotNull = columnDto.getIsNotNull();
        this.defaultVal = columnDto.getDefaultVal();
        this.comment = columnDto.getComment();
    }

    public ColumnMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.tableId = DataByteUtils.readInt(byteBuffer);
        this.columnName = readString(byteBuffer);
        this.columnType = byteBuffer.get();
        this.columnIndex = DataByteUtils.readInt(byteBuffer);
        this.columnLength = DataByteUtils.readInt(byteBuffer);
        this.isPrimaryKey = byteBuffer.get();
        this.comment = readString(byteBuffer);
        this.isNotNull = byteBuffer.get();
        this.defaultVal = readString(byteBuffer);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        DynamicByteBuffer byteBuffer = new DynamicByteBuffer();
        byteBuffer.putInt(totalByteLen);
        byteBuffer.putLong(startPos);
        byteBuffer.putInt(tableId);
        writeString(byteBuffer, columnName);
        byteBuffer.put(columnType);
        byteBuffer.putInt(columnIndex);
        byteBuffer.putInt(columnLength);
        byteBuffer.put(isPrimaryKey);
        writeString(byteBuffer, comment);
        byteBuffer.put(isNotNull);
        writeString(byteBuffer, defaultVal);
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, this.totalByteLen);
        return  byteBuffer.flipAndGetBuffer();
    }


    public int getTotalByteLen() {
        if(this.totalByteLen == 0) {
            getByteBuffer();
        }
        return this.totalByteLen;
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

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public byte getIsNotNull() {
        return isNotNull;
    }

    public void setIsNotNull(byte isNotNull) {
        this.isNotNull = isNotNull;
    }

    public String getDefaultVal() {
        return defaultVal;
    }

    public void setDefaultVal(String defaultVal) {
        this.defaultVal = defaultVal;
    }
}
