package com.moyu.xmz.store.type;

import com.moyu.xmz.store.common.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public interface DataType<T> {

    T read(ByteBuffer byteBuffer);

    void write(WriteBuffer writeBuffer, T value);

    int compare(T a, T b);

    int getMaxByteSize(T value);

}
