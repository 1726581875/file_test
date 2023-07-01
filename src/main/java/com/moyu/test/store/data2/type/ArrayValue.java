package com.moyu.test.store.data2.type;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/6/27
 */
public class ArrayValue<V> extends Value {

    private V[] arr;


    @Override
    public ByteBuffer getByteBuffer() {
        return null;
    }
}
