package com.moyu.xmz.store.common.block;

import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 * 表的索引块，包含一个表的所有索引元数据
 */
public class TableIndexBlock implements SerializableByte {

    public static final int TABLE_COLUMN_BLOCK_SIZE = 1024;

    private int usedByteLen;

    private int blockIndex;

    private long startPos;

    private int tableId;

    private int indexNum;

    private long indexStartPos;

    private List<IndexMeta> indexMetaList;

    public TableIndexBlock(int blockIndex, long startPos, int tableId) {
        this.usedByteLen = 32;
        this.blockIndex = blockIndex;
        this.startPos = startPos;
        this.tableId = tableId;
        this.indexNum = 0;
        this.indexStartPos = startPos + 32L;
        this.indexMetaList = new ArrayList<>();
    }


    public TableIndexBlock(ByteBuffer byteBuffer) {
        this.usedByteLen = DataByteUtils.readInt(byteBuffer);
        this.blockIndex = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.tableId = DataByteUtils.readInt(byteBuffer);
        this.indexNum = DataByteUtils.readInt(byteBuffer);
        this.indexStartPos = DataByteUtils.readLong(byteBuffer);
        this.indexMetaList = new ArrayList<>(indexNum);
        for (int i = 1; i <= indexNum; i++) {
            this.indexMetaList.add(new IndexMeta(byteBuffer));
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TABLE_COLUMN_BLOCK_SIZE);
        DataByteUtils.writeInt(byteBuffer, usedByteLen);
        DataByteUtils.writeInt(byteBuffer, blockIndex);
        DataByteUtils.writeLong(byteBuffer, startPos);
        DataByteUtils.writeInt(byteBuffer, tableId);
        DataByteUtils.writeInt(byteBuffer, indexNum);
        DataByteUtils.writeLong(byteBuffer, indexStartPos);
        for (int i = 0; i < this.indexMetaList.size(); i++) {
            IndexMeta index = this.indexMetaList.get(i);
            byteBuffer.put(index.getByteBuffer());
        }

        byteBuffer.rewind();
        return byteBuffer;
    }


    /**
     * @param index
     */
    public void addIndex(IndexMeta index) {
        this.indexNum++;
        // 更新长度
        index.getByteBuffer();
        this.usedByteLen += index.getTotalByteLen();
        this.indexMetaList.add(index);
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

    public int getIndexNum() {
        return indexNum;
    }

    public void setIndexNum(int indexNum) {
        this.indexNum = indexNum;
    }

    public long getIndexStartPos() {
        return indexStartPos;
    }

    public void setIndexStartPos(long indexStartPos) {
        this.indexStartPos = indexStartPos;
    }

    public List<IndexMeta> getIndexMetaList() {
        return indexMetaList;
    }

    public void setIndexMetaList(List<IndexMeta> indexMetaList) {
        this.indexMetaList = indexMetaList;
    }


    @Override
    public String toString() {
        return "ColumnIndexBlock{" +
                "usedByteLen=" + usedByteLen +
                ", blockIndex=" + blockIndex +
                ", startPos=" + startPos +
                ", tableId=" + tableId +
                ", columnNum=" + indexNum +
                ", columnStartPos=" + indexStartPos +
                ", indexMetadataList=" + indexMetaList +
                '}';
    }
}
