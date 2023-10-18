package com.moyu.test.store.type.dbtype;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class IntColumnType extends AbstractColumnType<Integer> {



    @Override
    protected Integer readValue(ByteBuffer byteBuffer) {
        return DataUtils.readInt(byteBuffer);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Integer value) {
        writeBuffer.putInt(value);
    }

    @Override
    public Class<?> getValueTypeClass() {
        return Integer.class;
    }

    @Override
    public int getMaxByteLen(Integer value) {
        return 4;
    }
}
