package com.moyu.xmz.store.accessor;

import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.PathUtils;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.RowMeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class DataChunkAccessor extends BaseAccessor {

    public static final int FIRST_BLOCK_INDEX = 1;
    /**
     * 最后一个数据块信息
     */
    private DataChunk lastChunk;
    /**
     * 块数量
     */
    private int dataChunkNum;
    /**
     * 当前最大行id
     */
    private long maxRowId;


    public DataChunkAccessor(String fileFullPath) throws IOException {
        super(fileFullPath);
        this.dataChunkNum = 0;
        // 初始化获取最后一个数据块到内存
        long endPosition = fileAccessor.getEndPosition();
        this.dataChunkNum = (int) (endPosition / DataChunk.DATA_CHUNK_LEN);

        if(fileAccessor.getEndPosition() >= 8) {
            this.maxRowId = DataByteUtils.readLong(fileAccessor.read(0, 8));
        }
        if (endPosition > DataChunk.DATA_CHUNK_LEN) {
            ByteBuffer readBuffer = fileAccessor.read(endPosition - DataChunk.DATA_CHUNK_LEN, DataChunk.DATA_CHUNK_LEN);
            this.lastChunk = new DataChunk(readBuffer);
        }

    }

    public DataChunk createChunk() {
        synchronized (this) {
            long startPos = lastChunk == null ? DataChunk.DATA_CHUNK_LEN : lastChunk.getStartPos() + DataChunk.DATA_CHUNK_LEN;
            int chunkIndex = lastChunk == null ? FIRST_BLOCK_INDEX : lastChunk.getChunkIndex() + 1;
            DataChunk dataChunk = new DataChunk(chunkIndex, startPos);
            ByteBuffer byteBuffer = dataChunk.getByteBuffer();
            fileAccessor.write(byteBuffer, startPos);
            this.lastChunk = dataChunk;
            this.dataChunkNum++;
            return dataChunk;
        }
    }

    public void updateChunk(DataChunk dataChunk) {
        synchronized (this) {
            ByteBuffer byteBuffer = dataChunk.getByteBuffer();
            fileAccessor.write(byteBuffer, dataChunk.getStartPos());
        }
    }

    public void truncateTable() {
        synchronized (this) {
            fileAccessor.truncate(0);
        }
    }


    public long getNextRowId() {
        synchronized (DataChunkAccessor.class) {
            this.maxRowId++;
            return this.maxRowId;
        }
    }

    public void updateMaxRowId() {
        synchronized (DataChunkAccessor.class) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
            DataByteUtils.writeLong(byteBuffer, this.maxRowId);
            byteBuffer.rewind();
            fileAccessor.write(byteBuffer, 0);
        }
    }

    /**
     *
     * @param row 数据字节
     * @param saveLastChunk 是否直接保存到最后一个数据块,true数据直接插入最后位置，不会从头遍历，提高插入效率
     * @return
     */
    public boolean storeRow(byte[] row, boolean saveLastChunk) {

        boolean result = false;
        if (saveLastChunk && lastChunk != null) {
            result = writeRow(row, lastChunk.getChunkIndex());
        } else {
            result = writeRow(row);
        }
        if (result == true) {
            return true;
        }
        // 创建一个数据块
        DataChunk chunk = createChunk();
        result = writeRow(row, chunk.getChunkIndex());
        if (result == true) {
            return true;
        }

        throw new RuntimeException("块空间不足");
    }

    public boolean storeRow(Column[] columns) {
        byte[] rowBytes = RowMeta.toRowByteData(columns);
        return storeRow(rowBytes, true);
    }

    public Long storeRowAndGetPos(Column[] columns, Long rowId) {
        byte[] rowBytes = RowMeta.toRowByteData(columns);
        if(lastChunk == null) {
            DataChunk chunk = createChunk();
            lastChunk = chunk;
        }
        int chunkIndex = lastChunk == null ? FIRST_BLOCK_INDEX : lastChunk.getChunkIndex();
        long startPos = chunkIndex * DataChunk.DATA_CHUNK_LEN;
        DataChunk dataChunk = writeRow(rowBytes, startPos, rowId);
        if (dataChunk != null) {
            updateMaxRowId();
            return dataChunk.getStartPos();
        }

        DataChunk newChunk = createChunk();
        DataChunk chunk = writeRow(rowBytes, newChunk.getStartPos(), rowId);
        if (chunk != null) {
            updateMaxRowId();
            return chunk.getStartPos();
        } else {
            return null;
        }
    }


    /**
     * 简单粗暴从头开始遍历，找到可以存放的数据块(剩余空间足够存储该行)。
     * 找到直接存储该行数据到对应数据块，如果目前所有数据块都存储不了返回false
     *
     * 优点可以重复利用前面的空间
     * 缺点是数据量大后插入数据效率比较慢
     * @param row
     * @return
     */
    private boolean writeRow(byte[] row) {
        long endPosition = fileAccessor.getEndPosition();
        if (endPosition >= DataChunk.DATA_CHUNK_LEN) {
            long currPos = 0;
            while (currPos < endPosition) {
                DataChunk dataChunk = writeRow(row, currPos);
                if (dataChunk != null) {
                    return true;
                }
                currPos += DataChunk.DATA_CHUNK_LEN;
            }
        }
        return false;
    }

    private boolean writeRow(byte[] row, int chunkIndex) {
        long startPos = chunkIndex * DataChunk.DATA_CHUNK_LEN;
        return writeRow(row, startPos) != null;
    }



    private DataChunk writeRow(byte[] row, long startPos) {
        return writeRow(row, startPos, null);
    }

    private DataChunk writeRow(byte[] row, long startPos, Long rowId) {
        ByteBuffer readBuffer = fileAccessor.read(startPos, DataChunk.DATA_CHUNK_LEN);
        DataChunk dataChunk = new DataChunk(readBuffer);
        Long rid = rowId == null ? getNextRowId() : rowId;
        RowMeta dataRow = new RowMeta(dataChunk.getNextRowStartPos(), row, rid);
        // 当前块剩余空间足够，直接存储到该块
        if (dataChunk.remaining() >= dataRow.getTotalByteLen()) {
            dataChunk.addRow(dataRow);
            fileAccessor.write(dataChunk.getByteBuffer(), dataChunk.getStartPos());
            return dataChunk;
        } else {
            return null;
        }
    }


    public int writeRow(List<byte[]> rows) {
        if(lastChunk == null) {
            lastChunk = createChunk();
        }
        int chunkIndex = lastChunk != null ? lastChunk.getChunkIndex() : FIRST_BLOCK_INDEX;
        long startPos = chunkIndex * DataChunk.DATA_CHUNK_LEN;
        ByteBuffer readBuffer = fileAccessor.read(startPos, DataChunk.DATA_CHUNK_LEN);
        DataChunk dataChunk = new DataChunk(readBuffer);
        for (int i = 0; i < rows.size(); i++) {
            byte[] row = rows.get(i);
            RowMeta dataRow = new RowMeta(dataChunk.getNextRowStartPos(), row, getNextRowId());
            // 当前块剩余空间足够，直接存储到该块
            if (dataChunk.remaining() >= dataRow.getTotalByteLen()) {
                dataChunk.addRow(dataRow);
            } else {
                fileAccessor.write(dataChunk.getByteBuffer(), dataChunk.getStartPos());
                dataChunk = createChunk();
                lastChunk = dataChunk;
                if (dataChunk.remaining() >= dataRow.getTotalByteLen()) {
                    dataChunk.addRow(dataRow);
                } else {
                    fileAccessor.write(dataChunk.getByteBuffer(), dataChunk.getStartPos());
                    return -1;
                }
            }
        }
        fileAccessor.write(dataChunk.getByteBuffer(), dataChunk.getStartPos());
        return 0;
    }


    public DataChunk getChunk(int i) {
        long startPos = i * DataChunk.DATA_CHUNK_LEN;
        if (i > dataChunkNum) {
            return null;
        }
        return getChunkByPos(startPos);
    }

    public DataChunk getChunkByPos(long startPos) {
        if (fileAccessor.getEndPosition() > startPos) {
            return new DataChunk(fileAccessor.read(startPos, DataChunk.DATA_CHUNK_LEN));
        }
        return null;
    }



    public DataChunk getLastChunk() {
        return lastChunk;
    }

    public void setLastChunk(DataChunk lastChunk) {
        this.lastChunk = lastChunk;
    }

    public int getDataChunkNum() {
        return dataChunkNum;
    }
}
