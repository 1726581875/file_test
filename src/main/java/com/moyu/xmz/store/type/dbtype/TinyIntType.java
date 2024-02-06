package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.common.DynByteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/12
 */
public class TinyIntType extends AbstractDbType<Byte> {



    @Override
    int getMaxByteLen(Byte value) {
        return 1;
    }

    @Override
    protected Byte readValue(ByteBuffer byteBuffer) {
        return byteBuffer.get();
    }

    @Override
    protected void writeValue(DynByteBuffer buffer, Byte value) {
        buffer.put(value);
    }

    @Override
    public Class<?> getValueTypeClass() {
        return Byte.class;
    }
}
