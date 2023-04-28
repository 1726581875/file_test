package com.moyu.test.store;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/26
 */
public class Chunk {
    /**
     * 4字节
     */
    private int chunkLen;
    /**
     * 4字节
     */
    private int chunkStartPos;
    /**
     * 下一个数据块开始位置，4字节
     */
    private int nextChunkPos;
    /**
     * 4字节
     */
    private int dataLen;

    private String data;


    public Chunk(int chunkStartPos, String data) {
        this.chunkStartPos = chunkStartPos;
        // chunkStartPos + chunkLen + nextChunkPos + dataLen = 16 再加上data的字节长度
        this.dataLen = getDateStringByteLength(data);
        this.chunkLen = 16 + dataLen;
        this.nextChunkPos = chunkStartPos + chunkLen;
        this.data = data;
    }


    public Chunk(ByteBuffer readBuff) {
        this.chunkLen = DataUtils.readInt(readBuff);
        this.chunkStartPos = DataUtils.readInt(readBuff);
        this.nextChunkPos = DataUtils.readInt(readBuff);
        this.dataLen = DataUtils.readInt(readBuff);
        this.data = DataUtils.readString(readBuff, dataLen);
    }

    private int getDateStringByteLength(String dataStr) {
        int len = 0;
        for (int i = 0; i < dataStr.length(); i++) {
            int c = dataStr.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c >= 0x800) {
                len += 3;
            } else {
                len += 2;
            }
        }
        return len;
    }


    public int getChunkStartPos() {
        return chunkStartPos;
    }

    public void setChunkStartPos(int chunkStartPos) {
        this.chunkStartPos = chunkStartPos;
    }

    public int getChunkLen() {
        return chunkLen;
    }

    public void setChunkLen(int chunkLen) {
        this.chunkLen = chunkLen;
    }

    public int getNextChunkPos() {
        return nextChunkPos;
    }

    public void setNextChunkPos(int nextChunkPos) {
        this.nextChunkPos = nextChunkPos;
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }


    public ByteBuffer getByteBuff() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(chunkLen);
        DataUtils.writeInt(byteBuffer, chunkLen);
        DataUtils.writeInt(byteBuffer, chunkStartPos);
        DataUtils.writeInt(byteBuffer, nextChunkPos);
        DataUtils.writeInt(byteBuffer, dataLen);
        DataUtils.writeStringData(byteBuffer, data, data.length());
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "Chunk{" +
                "chunkLen=" + chunkLen +
                ", chunkStartPos=" + chunkStartPos +
                ", nextChunkPos=" + nextChunkPos +
                ", dataLen=" + dataLen +
                ", data='" + data + '\'' +
                '}';
    }
}
