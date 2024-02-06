package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.common.DynByteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/11
 */
public class DoubleType extends AbstractDbType<Double> {

    @Override
    protected Double readValue(ByteBuffer byteBuffer) {
        return byteBuffer.getDouble();
    }

    @Override
    protected void writeValue(DynByteBuffer buffer, Double value) {
        buffer.putDouble(value);
    }

    @Override
    public Class<?> getValueTypeClass() {
        return Double.class;
    }

    @Override
    public int getMaxByteLen(Double value) {
        return 8;
    }
}
