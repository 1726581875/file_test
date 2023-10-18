package com.moyu.test.store.type.dbtype;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/10/17
 */
public class UnsignedLongColumnType extends AbstractColumnType<BigInteger> {

    @Override
    int getMaxByteLen(BigInteger value) {
        return 8;
    }

    @Override
    protected BigInteger readValue(ByteBuffer byteBuffer) {
        Long value = DataUtils.readLong(byteBuffer);
        return BigInteger.valueOf(value);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, BigInteger value) {
        writeBuffer.putLong(value.longValue());
    }

    @Override
    public Class<?> getValueTypeClass() {
        return BigInteger.class;
    }
}
