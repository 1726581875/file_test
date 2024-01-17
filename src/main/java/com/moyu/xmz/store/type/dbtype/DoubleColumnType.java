package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.store.common.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/11
 */
public class DoubleColumnType extends AbstractColumnType<Double> {

    @Override
    protected Double readValue(ByteBuffer byteBuffer) {
        return byteBuffer.getDouble();
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Double value) {
        writeBuffer.putDouble(value);
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
