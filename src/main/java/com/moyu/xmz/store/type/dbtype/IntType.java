package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class IntType extends AbstractDbType<Integer> {

    @Override
    protected Integer readValue(ByteBuffer byteBuffer) {
        return DataByteUtils.readInt(byteBuffer);
    }

    @Override
    protected void writeValue(DynByteBuffer buffer, Integer value) {
        buffer.putInt(value);
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
