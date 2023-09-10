package com.moyu.test.net.model.terminal;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.dbtype.AbstractColumnType;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class RowValueDto {

    private int totalByteLen;

    private Object[] columnValues;

    public RowValueDto(Object[] columnValues) {
        this.columnValues = columnValues;
    }

    public RowValueDto(ByteBuffer byteBuffer, ColumnDto[] columns) {
        this.totalByteLen = byteBuffer.getInt();
        this.columnValues = new Object[columns.length];
        this.columnValues = new Object[columns.length];
        for (int i = 0; i  < columns.length; i++) {
            byte columnType = columns[i].getColumnType();
            DataType dataType = AbstractColumnType.getDataType(columnType);
            columnValues[i] = dataType.read(byteBuffer);
        }
    }


    public ByteBuffer getByteBuffer(ColumnDto[] columns) {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(this.totalByteLen);
        for (int i = 0; i < columns.length; i++) {
            byte columnType = columns[i].getColumnType();
            DataType dataType = AbstractColumnType.getDataType(columnType);
            dataType.write(writeBuffer, this.columnValues[i]);
        }
        this.totalByteLen = writeBuffer.position();
        writeBuffer.putInt(0, this.totalByteLen);
        ByteBuffer buffer = writeBuffer.getBuffer();
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
