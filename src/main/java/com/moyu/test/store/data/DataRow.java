package com.moyu.test.store.data;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/11
 */
public class DataRow {

    private long totalByteLen;

    private long startPos;

    public DataRow(long totalByteLen, long startPos) {
        this.totalByteLen = totalByteLen;
        this.startPos = startPos;
    }


    public ByteBuffer getByteBuff() {
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) totalByteLen);
        DataUtils.writeLong(byteBuffer, this.totalByteLen);
        DataUtils.writeLong(byteBuffer, this.startPos);
        byteBuffer.rewind();
        return byteBuffer;
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

}
