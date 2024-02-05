package com.moyu.xmz.store.common.block;

import com.moyu.xmz.store.common.meta.ColumnMeta;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 * 表的字段块，包含一个表的所有字段元数据
 */
public class TableColumnBlock implements SerializableByte {

    public static final int TABLE_COLUMN_BLOCK_SIZE = 4096;

    private int usedByteLen;

    private int blockIndex;

    private long startPos;

    private int tableId;

    private int columnNum;

    private long columnStartPos;

    private List<ColumnMeta> columnMetaList;

    public TableColumnBlock(int blockIndex, long startPos, int tableId) {
        this.usedByteLen = 32;
        this.blockIndex = blockIndex;
        this.startPos = startPos;
        this.tableId = tableId;
        this.columnNum = 0;
        this.columnStartPos = startPos + 32L;
        this.columnMetaList = new ArrayList<>();
    }


    public TableColumnBlock(ByteBuffer byteBuffer) {
        this.usedByteLen = DataByteUtils.readInt(byteBuffer);
        this.blockIndex = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.tableId = DataByteUtils.readInt(byteBuffer);
        this.columnNum = DataByteUtils.readInt(byteBuffer);
        this.columnStartPos = DataByteUtils.readLong(byteBuffer);
        this.columnMetaList = new ArrayList<>(columnNum);
        for (int i = 1; i <= columnNum; i++) {
            this.columnMetaList.add(new ColumnMeta(byteBuffer));
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TABLE_COLUMN_BLOCK_SIZE);
        DataByteUtils.writeInt(byteBuffer, usedByteLen);
        DataByteUtils.writeInt(byteBuffer, blockIndex);
        DataByteUtils.writeLong(byteBuffer, startPos);
        DataByteUtils.writeInt(byteBuffer, tableId);
        DataByteUtils.writeInt(byteBuffer, columnNum);
        DataByteUtils.writeLong(byteBuffer, columnStartPos);
        for (int i = 0; i < this.columnMetaList.size(); i++) {
            ColumnMeta column = this.columnMetaList.get(i);
            byteBuffer.put(column.getByteBuffer());
        }

        byteBuffer.rewind();
        return byteBuffer;
    }


    /**
     * @param column
     */
    public void addColumn(ColumnMeta column) {
        this.columnNum++;
        this.usedByteLen += column.getTotalByteLen();
        this.columnMetaList.add(column);
    }

    public static int getTableColumnBlockSize() {
        return TABLE_COLUMN_BLOCK_SIZE;
    }

    public int getUsedByteLen() {
        return usedByteLen;
    }

    public void setUsedByteLen(int usedByteLen) {
        this.usedByteLen = usedByteLen;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(int blockIndex) {
        this.blockIndex = blockIndex;
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

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public long getColumnStartPos() {
        return columnStartPos;
    }

    public void setColumnStartPos(long columnStartPos) {
        this.columnStartPos = columnStartPos;
    }

    public List<ColumnMeta> getColumnMetaList() {
        return columnMetaList;
    }

    public void setColumnMetaList(List<ColumnMeta> columnMetaList) {
        this.columnMetaList = columnMetaList;
    }


    @Override
    public String toString() {
        return "TableColumnBlock{" +
                "usedByteLen=" + usedByteLen +
                ", blockIndex=" + blockIndex +
                ", startPos=" + startPos +
                ", tableId=" + tableId +
                ", columnNum=" + columnNum +
                ", columnStartPos=" + columnStartPos +
                ", columnMetadataList=" + columnMetaList +
                '}';
    }
}
