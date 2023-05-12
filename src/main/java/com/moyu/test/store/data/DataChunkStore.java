package com.moyu.test.store.data;

import com.moyu.test.store.FileStore;
import com.moyu.test.util.FileUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class DataChunkStore {

    private static final String defaultPath = "D:\\mytest\\fileTest\\";

    private static final String fileName = "data.m";

    private FileStore fileStore;

    private List<DataRow> dataRowList;

    private DataChunk lastChunk;

    private int dataChunkNum;


    public DataChunkStore(String fileFullPath) throws IOException {
        FileUtil.createFileIfNotExists(fileFullPath);
        this.fileStore = new FileStore(fileFullPath);
        this.dataChunkNum = 0;

        // 初始化最后一个数据块到内存
        long endPosition = fileStore.getEndPosition();
        if (endPosition >= DataChunk.DATA_CHUNK_LEN) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileStore.read(currPos, DataChunk.DATA_CHUNK_LEN);
                this.lastChunk = new DataChunk(readBuffer);
                this.dataChunkNum++;
                currPos += DataChunk.DATA_CHUNK_LEN;
            }
        }
    }


    public DataChunkStore() throws IOException {

    }


    public void createChunk() {
        synchronized (this) {
            long startPos = lastChunk == null ? 0L : lastChunk.getStartPos() + DataChunk.DATA_CHUNK_LEN;
            int chunkIndex = lastChunk == null ? 0 : lastChunk.getChunkIndex() + 1;
            DataChunk dataChunk = new DataChunk(chunkIndex, startPos);
            ByteBuffer byteBuffer = dataChunk.getByteBuffer();
            fileStore.write(byteBuffer, startPos);
            this.lastChunk = dataChunk;
            this.dataChunkNum++;
        }
    }


    public boolean addRow(byte[] row) {
        boolean result = writeRow(row);
        if (result == true) {
            return true;
        }
        throw new RuntimeException("块空间不足");
    }


    private boolean writeRow(byte[] row) {
        long endPosition = fileStore.getEndPosition();
        if (endPosition >= DataChunk.DATA_CHUNK_LEN) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileStore.read(currPos, DataChunk.DATA_CHUNK_LEN);
                DataChunk dataChunk = new DataChunk(readBuffer);
                DataRow dataRow = new DataRow(dataChunk.getNextRowStartPos(), row);
                // 当前块剩余空间足够，直接存储到该块
                if (dataChunk.remaining() >= dataRow.getTotalByteLen()) {
                    dataChunk.addRow(dataRow);
                    fileStore.write(dataChunk.getByteBuffer(), dataChunk.getStartPos());
                    return true;
                }
                currPos += DataChunk.DATA_CHUNK_LEN;
            }
        }
        return false;
    }


    public DataChunk getChunk(int i) {
        long startPos = i * DataChunk.DATA_CHUNK_LEN;
        if (i > dataChunkNum - 1) {
            return null;
        }
        if (fileStore.getEndPosition() >= startPos) {
            return new DataChunk(fileStore.read(startPos, DataChunk.DATA_CHUNK_LEN));
        }
        return null;
    }



    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }


    public DataChunk getLastChunk() {
        return lastChunk;
    }

    public void setLastChunk(DataChunk lastChunk) {
        this.lastChunk = lastChunk;
    }
}
