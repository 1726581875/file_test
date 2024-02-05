package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.store.common.WriteBuffer;
import com.moyu.xmz.common.util.DataByteUtils;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/10/17
 */
public class UnsignedIntType extends AbstractDbType<Long> {

    @Override
    int getMaxByteLen(Long value) {
        return 8;
    }

    @Override
    protected Long readValue(ByteBuffer byteBuffer) {
        Long value = DataByteUtils.readLong(byteBuffer);
        return value;
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Long value) {
        writeBuffer.putLong(value);
    }

    @Override
    public Class<?> getValueTypeClass() {
        return Long.class;
    }
}
