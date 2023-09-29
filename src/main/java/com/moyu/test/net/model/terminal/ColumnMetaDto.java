package com.moyu.test.net.model.terminal;

import com.moyu.test.net.util.ReadWriteUtil;
import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class ColumnMetaDto {

    private int totalByteLen;

    private String columnName;

    private String alias;

    private String tableAlias;

    private byte columnType;


    public ColumnMetaDto(String columnName, String alias, String tableAlias, byte columnType) {
        this.totalByteLen = 0;
        this.columnName = columnName;
        this.alias = alias;
        this.tableAlias = tableAlias;
        this.columnType = columnType;
    }

    public ColumnMetaDto(ByteBuffer byteBuffer) {
        this.totalByteLen = byteBuffer.getInt();
        this.columnName = ReadWriteUtil.readString(byteBuffer);
        this.alias = ReadWriteUtil.readString(byteBuffer);
        this.tableAlias = ReadWriteUtil.readString(byteBuffer);
        this.columnType = byteBuffer.get();
    }


    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(totalByteLen);
        ReadWriteUtil.writeString(writeBuffer, columnName);
        ReadWriteUtil.writeString(writeBuffer, alias);
        ReadWriteUtil.writeString(writeBuffer, tableAlias);
        writeBuffer.put(columnType);
        totalByteLen = writeBuffer.position();
        writeBuffer.putInt(0, totalByteLen);
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }



    public int getTotalByteLen() {
        return totalByteLen;
    }

    public void setTotalByteLen(int totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public byte getColumnType() {
        return columnType;
    }

    public void setColumnType(byte columnType) {
        this.columnType = columnType;
    }

    @Override
    public String toString() {
        return "ColumnDto{" +
                "totalByteLen=" + totalByteLen +
                ", columnName='" + columnName + '\'' +
                ", alias='" + alias + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                ", columnType=" + columnType +
                '}';
    }
}
