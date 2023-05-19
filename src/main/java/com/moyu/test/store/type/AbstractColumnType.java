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
        // 判断标记位，0表示值为null、1不为null
        byte flag = byteBuffer.get();
        if (flag == (byte) 0) {
            return null;
        } else {
            return readValue(byteBuffer);
        }
    }

    @Override
    public void write(WriteBuffer writeBuffer, T value) {
        // 如果是空值(null)，写入标记位值0
        if (value == null) {
            byte flag = 0;
            writeBuffer.put(flag);
        } else {
            byte flag = 1;
            writeBuffer.put(flag);
            writeValue(writeBuffer, value);
        }
    }

    /**
     * 把值写入ByteBuffer
     * @return
     */
    protected abstract T readValue(ByteBuffer byteBuffer);

    protected abstract void writeValue(WriteBuffer writeBuffer, T value);


}
