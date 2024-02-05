package com.moyu.xmz.store.type.value;

import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.type.ColumnTypeFactory;
import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.AbstractDbType;
import com.moyu.xmz.store.type.obj.RowDataType;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.WriteBuffer;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/6/30
 */
public class RowValue extends Value {
    /**
     * 该元组一共占用字节数
     */
    private int totalByteLen;
    /**
     * 当前行所在文件物理位置偏移
     * TODO 是否要保留，或者不直接指向文件中位置？应当只指向块内地址偏移？
     */
    private long startPos;
    /**
     * 数据字节长度row
     */
    private int rowByteLen;
    /**
     * 数据行id
     */
    private long rowId;

    /**
     * 数据是否已删除
     * 0 否
     * 1 是
     */
    private byte isDeleted;

    /**
     * 包含该行所有字段的数据
     * 存储格式是按列字段顺序和值，byte数组是[字段a的值描述 + 字段a的值描述 + 字段..的描述]
     *
     *  例1: 存储一个字段a，类型为int，值为3。其描述如下
     *   1字节标记位 [000000001]  + 4字节int [00000000 00000000 00000000 00000011]
     *
     *  例2: 存储字段b，类型为varchar，值为'A'。其描述如下
     *  1字节标记位 [000000001]  + 4字节int字符长度 [00000000 00000000 00000000 00000001]  + 字符内容‘A’对应的二进制字节 [1000001]
     *
     *  标记位是标记该值是否为空,0表示值为空、1表示值非空
     */
    private byte[] columnBytes;


    public RowValue(long startPos, byte[] columnBytes) {
        this.startPos = startPos;
        this.columnBytes = columnBytes;
        this.rowByteLen = columnBytes.length;
        this.totalByteLen = 25 + columnBytes.length;
    }

    public RowValue(long startPos, Column[] columns, long rowId) {
        this.startPos = startPos;
        this.columnBytes = columnDataToBytes(columns);
        this.rowByteLen = columnBytes.length;
        this.rowId = rowId;
        this.isDeleted = 0;
        this.totalByteLen = 25 + columnBytes.length;
    }

    public RowValue(ByteBuffer byteBuffer) {
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.rowByteLen = DataByteUtils.readInt(byteBuffer);
        this.rowId = DataByteUtils.readLong(byteBuffer);
        this.isDeleted = byteBuffer.get();
        byte[] row = new byte[rowByteLen];
        byteBuffer.get(row);
        this.columnBytes = row;
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalByteLen);
        DataByteUtils.writeInt(byteBuffer, this.totalByteLen);
        DataByteUtils.writeLong(byteBuffer, this.startPos);
        DataByteUtils.writeInt(byteBuffer, this.rowByteLen);
        DataByteUtils.writeLong(byteBuffer, this.rowId);
        byteBuffer.put(isDeleted);
        byteBuffer.put(this.columnBytes);
        byteBuffer.rewind();
        return byteBuffer;
    }


    public void setColumns(Column[] columns) {
        this.columnBytes = columnDataToBytes(columns);
        this.rowByteLen = columnBytes.length;
        this.totalByteLen = 25 + columnBytes.length;
    }

    public void setIsDeleted(byte isDeleted) {
        this.isDeleted = isDeleted;
    }

    private byte[] columnDataToBytes(Column[] columns) {
        WriteBuffer writeBuffer = new WriteBuffer(16);
        for (Column column : columns) {
            DataType columnType = ColumnTypeFactory.getColumnType(column.getColumnType());
            Object value = convertValue((AbstractDbType) columnType, column.getValue());
            columnType.write(writeBuffer, value);
        }
        writeBuffer.getBuffer().flip();
        byte[] result = new byte[writeBuffer.limit()];
        writeBuffer.get(result);
        return result;
    }

    private Object convertValue(AbstractDbType columnType, Object value) {
        if(value == null) {
            return value;
        }
        if(columnType.getValueTypeClass().equals(value.getClass())) {
            return value;
        }

        if(Byte.class.equals(columnType.getValueTypeClass())) {
           if(value instanceof Integer) {
               int v = ((Integer) value).intValue();
               if(v > 127 || v < -128) {
                   throw new DbException("int不能转换为byte, value=" + value);
               }
               return (byte)((Integer) value).intValue();
           }
        }
        return value;
    }


    public RowEntity getRowEntity(Column[] columns) {
        Column[] resultColumns = new Column[columns.length];
        ByteBuffer byteBuffer = ByteBuffer.wrap(columnBytes);
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            DataType columnType = ColumnTypeFactory.getColumnType(column.getColumnType());
            Object value = columnType.read(byteBuffer);

            Column valueColumn = column.createNullValueColumn();
            valueColumn.setValue(value);
            resultColumns[i] = valueColumn;
        }
        RowEntity rowEntity = new RowEntity(resultColumns, rowId, this.isDeleted == (byte) 1);
        return rowEntity;
    }


    public int getTotalByteLen() {
        return totalByteLen;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }


    @Override
    public String toString() {
        return "DataRow{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", rowByteLen=" + rowByteLen +
                ", row=" + Arrays.toString(columnBytes) +
                '}';
    }

    @Override
    public DataType getDataTypeObj() {
        return new RowDataType();
    }

    @Override
    public int getType() {
        return TYPE_ROW_VALUE;
    }

    @Override
    public Object getObjValue() {
        return null;
    }

    @Override
    public int compare(Value v) {
        return 0;
    }

    @Override
    public int getMaxSize() {
        return totalByteLen;
    }
}
