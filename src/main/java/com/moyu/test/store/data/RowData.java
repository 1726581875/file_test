package com.moyu.test.store.data;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.ColumnType;
import com.moyu.test.store.type.ColumnTypeFactory;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/11
 * 行数据，每一行数据(元组)的所有字节
 */
public class RowData {

    private long totalByteLen;

    private long startPos;

    private int rowByteLen;

    /**
     * 包含该行所有字段的数据
     */
    private byte[] row;


    public RowData(long startPos, byte[] row) {
        this.startPos = startPos;
        this.rowByteLen = row.length;
        this.row = row;
        this.totalByteLen = 20 + row.length;
    }

    public RowData(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readLong(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.rowByteLen = DataUtils.readInt(byteBuffer);
        byte[] row = new byte[rowByteLen];
        byteBuffer.get(row);
        this.row = row;
    }


    public ByteBuffer getByteBuff() {
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) totalByteLen);
        DataUtils.writeLong(byteBuffer, this.totalByteLen);
        DataUtils.writeLong(byteBuffer, this.startPos);
        DataUtils.writeInt(byteBuffer, this.rowByteLen);
        byteBuffer.put(this.row);
        byteBuffer.rewind();
        return byteBuffer;
    }


    public static byte[] toRowByteData(List<Column> columnList) {
        Column[] columns = columnList.toArray(new Column[0]);
        return toRowByteData(columns);
    }

    public static byte[] toRowByteData(Column[] columns) {
        WriteBuffer writeBuffer = new WriteBuffer(16);
        for (Column column : columns) {
            ColumnType columnType = ColumnTypeFactory.getColumnType(column.getColumnType());
            columnType.write(writeBuffer, column.getValue());
        }
        writeBuffer.getBuffer().flip();
        byte[] result = new byte[writeBuffer.limit()];
        writeBuffer.get(result);
        return result;
    }

    public List<Column> getColumnList(List<Column> columnList) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(row);
        for (Column column : columnList) {
            ColumnType columnType = ColumnTypeFactory.getColumnType(column.getColumnType());
            Object value = columnType.read(byteBuffer);
            column.setValue(value);
        }
        return columnList;
    }


    public long getTotalByteLen() {
        return totalByteLen;
    }

    public void setTotalByteLen(long totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getRowByteLen() {
        return rowByteLen;
    }

    public void setRowByteLen(int rowByteLen) {
        this.rowByteLen = rowByteLen;
    }

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    @Override
    public String toString() {
        return "DataRow{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", rowByteLen=" + rowByteLen +
                ", row=" + Arrays.toString(row) +
                '}';
    }
}
