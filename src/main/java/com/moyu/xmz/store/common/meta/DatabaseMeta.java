package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class DatabaseMeta extends AbstractMeta implements SerializableByte {

    private int totalByteLen;

    private long startPos;

    private int databaseId;

    private String name;


    public DatabaseMeta(String name, int databaseId, long startPos) {
        this.startPos = startPos;
        this.databaseId = databaseId;
        this.name = name;
    }

    public DatabaseMeta(ByteBuffer byteBuffer) {
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.databaseId = DataByteUtils.readInt(byteBuffer);
        this.name = readString(byteBuffer);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        DynByteBuffer byteBuffer = new DynByteBuffer();
        byteBuffer.putInt(this.totalByteLen);
        byteBuffer.putLong(this.startPos);
        byteBuffer.putInt(this.databaseId);
        writeString(byteBuffer, this.name);
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, totalByteLen);
        return byteBuffer.flipAndGetBuffer();
    }

    public int getTotalByteLen() {
        if(totalByteLen == 0) {
            getByteBuffer();
        }
        return totalByteLen;
    }

    public void setTotalByteLen(int totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public int getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    @Override
    public String toString() {
        return "DatabaseMetadata{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", databaseId=" + databaseId +
                ", name='" + name + '\'' +
                '}';
    }
}
