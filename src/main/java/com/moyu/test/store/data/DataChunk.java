package com.moyu.test.store.data;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/11
 */
public class DataChunk {

    public static final int dataChunkLen = 1024;

    private long totalByteLen;

    private int chunkIndex;

    private int rowNum;

    private long startPos;

    private long dataStartPos;

    private List<DataRow> dataRowList;

    public DataChunk(int chunkIndex, long startPos) {
        // 8(totalByteLen) + 4(chunkIndex) + 4(rowNum) + 8(startPos) + 8(dataStartPos) = 32
        this.totalByteLen = 32;
        this.chunkIndex = chunkIndex;
        this.rowNum = 0;
        this.startPos = startPos;
        this.dataStartPos = this.totalByteLen;
        this.dataRowList = new ArrayList<>();
    }

    public DataChunk(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readLong(byteBuffer);
        this.chunkIndex = DataUtils.readInt(byteBuffer);
        this.rowNum = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.dataStartPos = DataUtils.readLong(byteBuffer);
        //this.dataRowList = new ArrayList<>();
    }


    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(dataChunkLen);
        DataUtils.writeLong(byteBuffer, this.totalByteLen);
        DataUtils.writeInt(byteBuffer, this.chunkIndex);
        DataUtils.writeInt(byteBuffer, this.rowNum);
        DataUtils.writeLong(byteBuffer, this.startPos);
        DataUtils.writeLong(byteBuffer, this.dataStartPos);

        if (this.dataRowList != null && this.dataRowList.size() > 0) {
            for (int i = 0; i < this.dataRowList.size(); i++) {
                byteBuffer.put(dataRowList.get(i).getByteBuff());
            }
        }
        byteBuffer.rewind();

        return byteBuffer;
    }

}
