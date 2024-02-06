package com.moyu.xmz.store.type.obj;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.store.type.value.ArrayValue;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/8
 */
public class ArrayDataType extends AbstractObjType<ArrayValue> {

    @Override
    public ArrayValue read(ByteBuffer byteBuffer) {
        return new ArrayValue(byteBuffer);
    }

    @Override
    public void write(DynByteBuffer buffer, ArrayValue value) {
        buffer.put(value.getByteBuffer());
    }

    @Override
    public int compare(ArrayValue a, ArrayValue b) {
        return 0;
    }

    @Override
    public int getMaxByteSize(ArrayValue value) {
        return value.getMaxSize();
    }
}
