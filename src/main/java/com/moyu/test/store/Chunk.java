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
    private int chunkStartPos;
    /**
     * 4字节
     */
    private int chunkLen;
    /**
     * 下一个数据块开始位置，4字节
     */
    private int nextChunkPos;
    /**
     * 4字节
     * todo 数据长度不准确，实际数据长度小于或等于dataLen，暂时为了方便读取假设每个字符都按3字节存储了
     */
    private int dataLen;

    private String data;


    public Chunk(int chunkStartPos, String data) {
        this.chunkStartPos = chunkStartPos;
        // chunkStartPos + chunkLen + nextChunkPos + dataLen = 16 再加上data的字节长度
        this.dataLen = data.length() * 3;
        this.chunkLen = 16 + dataLen;
        this.nextChunkPos = chunkStartPos + chunkLen;
        this.data = data;
    }


    public Chunk(ByteBuffer readBuff) {
        this.chunkStartPos = DataUtils.readInt(readBuff);
        this.chunkLen = DataUtils.readInt(readBuff);
        this.nextChunkPos = DataUtils.readInt(readBuff);
        this.dataLen = DataUtils.readInt(readBuff);
        this.data = DataUtils.readString(readBuff, dataLen);
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
        DataUtils.writeInt(byteBuffer, chunkStartPos);
        DataUtils.writeInt(byteBuffer, chunkLen);
        DataUtils.writeInt(byteBuffer, nextChunkPos);
        DataUtils.writeInt(byteBuffer, dataLen);
        DataUtils.writeStringData(byteBuffer, data, data.length());
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "Chunk{" +
                "chunkStartPos=" + chunkStartPos +
                ", chunkLen=" + chunkLen +
                ", nextChunkPos=" + nextChunkPos +
                ", dataLen=" + dataLen +
                ", data='" + data + '\'' +
                '}';
    }
}
