package com.moyu.test.store.metadata.obj;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class TableColumnBlock {

    public static final int TABLE_COLUMN_BLOCK_SIZE = 1024;

    private int usedByteLen;

    private int blockIndex;

    private long startPos;

    private int tableId;

    private int columnNum;

    private long columnStartPos;

    private List<ColumnMetadata> columnMetadataList;

    public TableColumnBlock(int blockIndex,
                            long startPos,
                            int tableId) {
        this.usedByteLen = 32;
        this.blockIndex = blockIndex;
        this.startPos = startPos;
        this.tableId = tableId;
        this.columnNum = 0;
        this.columnStartPos = startPos + 32L;
        this.columnMetadataList = new ArrayList<>();
    }


    public TableColumnBlock(ByteBuffer byteBuffer) {
        this.usedByteLen = DataUtils.readInt(byteBuffer);
        this.blockIndex = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        this.columnNum = DataUtils.readInt(byteBuffer);
        this.columnStartPos = DataUtils.readLong(byteBuffer);
        this.columnMetadataList = new ArrayList<>(columnNum);
        for (int i = 1; i <= columnNum; i++) {
            this.columnMetadataList.add(new ColumnMetadata(byteBuffer));
        }
    }


    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TABLE_COLUMN_BLOCK_SIZE);
        DataUtils.writeInt(byteBuffer, usedByteLen);
        DataUtils.writeInt(byteBuffer, blockIndex);
        DataUtils.writeLong(byteBuffer, startPos);
        DataUtils.writeInt(byteBuffer, tableId);
        DataUtils.writeInt(byteBuffer, columnNum);
        DataUtils.writeLong(byteBuffer, columnStartPos);
        for (int i = 0; i < this.columnMetadataList.size(); i++) {
            ColumnMetadata column = this.columnMetadataList.get(i);
            byteBuffer.put(column.getByteBuffer());
        }

        byteBuffer.rewind();
        return byteBuffer;
    }


    /**
     * @param column
     */
    public void addColumn(ColumnMetadata column) {
        this.columnNum++;
        this.usedByteLen += column.getTotalByteLen();
        this.columnMetadataList.add(column);
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

    public List<ColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void setColumnMetadataList(List<ColumnMetadata> columnMetadataList) {
        this.columnMetadataList = columnMetadataList;
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
                ", columnMetadataList=" + columnMetadataList +
                '}';
    }
}
