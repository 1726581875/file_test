package com.moyu.test.store.type.dbtype;

import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/12
 */
public class TinyIntColumnType extends AbstractColumnType<Byte> {
    @Override
    int getMaxByteLen(Byte value) {
        return 1;
    }

    @Override
    protected Byte readValue(ByteBuffer byteBuffer) {
        return byteBuffer.get();
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Byte value) {
        writeBuffer.put(value);
    }
}
