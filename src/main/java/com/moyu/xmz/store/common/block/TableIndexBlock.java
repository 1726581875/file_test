package com.moyu.xmz.store.common.block;

import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class TableIndexBlock implements SerializableByte {

    public static final int TABLE_COLUMN_BLOCK_SIZE = 1024;

    private int usedByteLen;

    private int blockIndex;

    private long startPos;

    private int tableId;

    private int indexNum;

    private long indexStartPos;

    private List<IndexMetadata> indexMetadataList;

    public TableIndexBlock(int blockIndex, long startPos, int tableId) {
        this.usedByteLen = 32;
        this.blockIndex = blockIndex;
        this.startPos = startPos;
        this.tableId = tableId;
        this.indexNum = 0;
        this.indexStartPos = startPos + 32L;
        this.indexMetadataList = new ArrayList<>();
    }


    public TableIndexBlock(ByteBuffer byteBuffer) {
        this.usedByteLen = DataUtils.readInt(byteBuffer);
        this.blockIndex = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.tableId = DataUtils.readInt(byteBuffer);
        this.indexNum = DataUtils.readInt(byteBuffer);
        this.indexStartPos = DataUtils.readLong(byteBuffer);
        this.indexMetadataList = new ArrayList<>(indexNum);
        for (int i = 1; i <= indexNum; i++) {
            this.indexMetadataList.add(new IndexMetadata(byteBuffer));
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TABLE_COLUMN_BLOCK_SIZE);
        DataUtils.writeInt(byteBuffer, usedByteLen);
        DataUtils.writeInt(byteBuffer, blockIndex);
        DataUtils.writeLong(byteBuffer, startPos);
        DataUtils.writeInt(byteBuffer, tableId);
        DataUtils.writeInt(byteBuffer, indexNum);
        DataUtils.writeLong(byteBuffer, indexStartPos);
        for (int i = 0; i < this.indexMetadataList.size(); i++) {
            IndexMetadata index = this.indexMetadataList.get(i);
            byteBuffer.put(index.getByteBuffer());
        }

        byteBuffer.rewind();
        return byteBuffer;
    }


    /**
     * @param index
     */
    public void addIndex(IndexMetadata index) {
        this.indexNum++;
        // 更新长度
        index.getByteBuffer();
        this.usedByteLen += index.getTotalByteLen();
        this.indexMetadataList.add(index);
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

    public List<IndexMetadata> getIndexMetadataList() {
        return indexMetadataList;
    }

    public void setIndexMetadataList(List<IndexMetadata> indexMetadataList) {
        this.indexMetadataList = indexMetadataList;
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
                ", indexMetadataList=" + indexMetadataList +
                '}';
    }
}
