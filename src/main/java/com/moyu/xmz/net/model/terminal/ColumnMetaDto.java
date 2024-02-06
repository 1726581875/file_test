package com.moyu.xmz.net.model.terminal;

import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.common.DynByteBuffer;

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
        DynByteBuffer dynByteBuffer = new DynByteBuffer();
        dynByteBuffer.putInt(totalByteLen);
        ReadWriteUtil.writeString(dynByteBuffer, columnName);
        ReadWriteUtil.writeString(dynByteBuffer, alias);
        ReadWriteUtil.writeString(dynByteBuffer, tableAlias);
        dynByteBuffer.put(columnType);
        totalByteLen = dynByteBuffer.position();
        dynByteBuffer.putInt(0, totalByteLen);
        ByteBuffer buffer = dynByteBuffer.getBuffer();
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
