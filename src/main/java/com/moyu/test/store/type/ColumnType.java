package com.moyu.test.store.type;

import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public interface ColumnType<T> {

    T read(ByteBuffer byteBuffer);

    void write(WriteBuffer writeBuffer, T value);

}
