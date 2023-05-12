package com.moyu.test.store.type;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public abstract class AbstractColumnType<T> implements ColumnType<T> {


    @Override
    public T read(ByteBuffer byteBuffer) {
        expand(byteBuffer);
        return readValue(byteBuffer);
    }

    @Override
    public void write(ByteBuffer byteBuffer, T value) {
        expand(byteBuffer);
        writeValue(byteBuffer, value);
    }

    /**
     * 如果byteBuffer长度不够，需要扩容
     * @param byteBuffer
     */
    protected abstract void expand(ByteBuffer byteBuffer);

    /**
     * 把值写入ByteBuffer
     * @return
     */
    protected abstract T readValue(ByteBuffer byteBuffer);

    protected abstract void writeValue(ByteBuffer byteBuffer, T value);


    /**
     * 按固定长度增加容量
     * @param byteBuffer
     * @param len
     */
    protected void expand(ByteBuffer byteBuffer, int len) {
        ByteBuffer newBuffer = ByteBuffer.allocate(byteBuffer.capacity() + len);
        byteBuffer.flip();
        newBuffer.put(byteBuffer);
        byteBuffer = newBuffer;
    }


}
