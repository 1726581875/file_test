package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.common.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/5
 */
public class DatabaseMetadata implements SerializableByte {

    private int totalByteLen;

    private long startPos;

    private int databaseId;

    private int nameByteLen;

    private int nameCharLen;

    private String name;


    public DatabaseMetadata(String name, int databaseId, long startPos) {
        this.nameByteLen = DataUtils.getDateStringByteLength(name);
        this.startPos = startPos;
        this.nameCharLen = name.length();
        this.databaseId = databaseId;
        this.name = name;
        this.totalByteLen = 4 + 8 + 4 + 4 + 4 + this.nameByteLen;
    }

    public DatabaseMetadata(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.databaseId = DataUtils.readInt(byteBuffer);
        this.nameByteLen = DataUtils.readInt(byteBuffer);
        this.nameCharLen = DataUtils.readInt(byteBuffer);
        this.name = DataUtils.readString(byteBuffer, this.nameCharLen);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalByteLen);
        DataUtils.writeInt(byteBuffer, totalByteLen);
        DataUtils.writeLong(byteBuffer, startPos);
        DataUtils.writeInt(byteBuffer, databaseId);
        DataUtils.writeInt(byteBuffer, nameByteLen);
        DataUtils.writeInt(byteBuffer, nameCharLen);
        DataUtils.writeStringData(byteBuffer, name, name.length());
        byteBuffer.rewind();
        return byteBuffer;
    }

    public int getTotalByteLen() {
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

    public int getNameByteLen() {
        return nameByteLen;
    }

    public void setNameByteLen(int nameByteLen) {
        this.nameByteLen = nameByteLen;
    }

    public int getNameCharLen() {
        return nameCharLen;
    }

    public void setNameCharLen(int nameCharLen) {
        this.nameCharLen = nameCharLen;
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
                ", nameByteLen=" + nameByteLen +
                ", nameCharLen=" + nameCharLen +
                ", name='" + name + '\'' +
                '}';
    }
}
