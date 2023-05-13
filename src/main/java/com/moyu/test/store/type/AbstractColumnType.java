package com.moyu.test.store.type;

import com.moyu.test.store.WriteBuffer;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public abstract class AbstractColumnType<T> implements ColumnType<T> {


    @Override
    public T read(ByteBuffer byteBuffer) {
        return readValue(byteBuffer);
    }

    @Override
    public void write(WriteBuffer writeBuffer, T value) {
        writeValue(writeBuffer, value);
    }

    /**
     * 把值写入ByteBuffer
     * @return
     */
    protected abstract T readValue(ByteBuffer byteBuffer);

    protected abstract void writeValue(WriteBuffer writeBuffer, T value);


}
