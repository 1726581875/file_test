package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.store.common.WriteBuffer;

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

    @Override
    public Class<?> getValueTypeClass() {
        return Byte.class;
    }
}
