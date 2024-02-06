package com.moyu.xmz.store.type;

import com.moyu.xmz.common.DynByteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public interface DataType<T> {

    T read(ByteBuffer byteBuffer);

    void write(DynByteBuffer buffer, T value);

    int compare(T a, T b);

    int getMaxByteSize(T value);

}
