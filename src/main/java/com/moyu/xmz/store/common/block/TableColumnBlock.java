package com.moyu.xmz.store.common.block;

import com.moyu.xmz.common.exception.ExceptionUtil;
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
    /**
     * 已使用字节长度，目前限制不得超过块大小4096字节
     */
    private int usedByteLen;
    /**
     * 当前块下标
     */
    private int blockIndex;
    /**
     * 当前文件内偏移量
     * blockIndex * 块大小(4096)
     */
    private long startPos;
    /**
     * 所属表id
     */
    private int tableId;
    /**
     * 列字段数量
     */
    private int columnNum;
    /**
     * 字段信息开始偏移量
     */
    private long columnStartPos;
    /**
     * 具体字段数据
     */
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

    public void addColumn(ColumnMeta column, int i) {
        this.columnNum++;
        this.usedByteLen += column.getTotalByteLen();
        this.columnMetaList.add(i, column);
    }

    public void removeColumn(String columnName) {
        int removeIdx = -1;
        for (int i = 0; i < columnMetaList.size(); i++) {
            if(columnMetaList.get(i).getColumnName().equals(columnName)) {
                removeIdx = i;
            }
        }
        if(removeIdx == -1) {
            ExceptionUtil.throwDbException("删除字段失败，不存在字段{}", columnName);
        }

        ColumnMeta columnMeta = columnMetaList.get(removeIdx);

        this.usedByteLen -= columnMeta.getTotalByteLen();
        this.columnNum--;
        this.columnMetaList.remove(removeIdx);
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
