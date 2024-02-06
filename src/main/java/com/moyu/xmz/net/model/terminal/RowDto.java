package com.moyu.xmz.net.model.terminal;

import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.AbstractDbType;
import com.moyu.xmz.common.DynByteBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class RowDto {

    private int totalByteLen;

    private Object[] columnValues;

    public RowDto(Object[] columnValues) {
        this.columnValues = columnValues;
    }

    public RowDto(ByteBuffer byteBuffer, ColumnMetaDto[] columns) {
        this.totalByteLen = byteBuffer.getInt();
        this.columnValues = new Object[columns.length];
        this.columnValues = new Object[columns.length];
        for (int i = 0; i  < columns.length; i++) {
            byte columnType = columns[i].getColumnType();
            DataType dataType = AbstractDbType.getDataType(columnType);
            columnValues[i] = dataType.read(byteBuffer);
        }
    }


    public ByteBuffer getByteBuffer(ColumnMetaDto[] columns) {
        DynByteBuffer dynByteBuffer = new DynByteBuffer();
        dynByteBuffer.putInt(this.totalByteLen);
        for (int i = 0; i < columns.length; i++) {
            byte columnType = columns[i].getColumnType();
            DataType dataType = AbstractDbType.getDataType(columnType);
            dataType.write(dynByteBuffer, this.columnValues[i]);
        }
        this.totalByteLen = dynByteBuffer.position();
        dynByteBuffer.putInt(0, this.totalByteLen);
        ByteBuffer buffer = dynByteBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }

    public Object[] getColumnValues() {
        return this.columnValues;
    }

    @Override
    public String toString() {
        return "RowValueDto{" +
                "totalByteLen=" + totalByteLen +
                ", columnValues=" + Arrays.toString(columnValues) +
                '}';
    }
}
