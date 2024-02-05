package com.moyu.xmz.store.type.obj;
import com.moyu.xmz.store.common.WriteBuffer;
import com.moyu.xmz.store.type.DataType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class LongArrayType implements DataType<Long[]> {

    @Override
    public Long[] read(ByteBuffer byteBuffer) {
        int size = byteBuffer.getInt();
        Long[] arr = new Long[size];
        for (int i = 0; i < size; i++) {
            arr[i] = byteBuffer.getLong();
        }
        return arr;
    }

    @Override
    public void write(WriteBuffer writeBuffer, Long[] value) {
        int size = value.length;
        writeBuffer.putInt(size);
        for (int i = 0; i < size; i++) {
            writeBuffer.putLong(value[i]);
        }
    }

    @Override
    public int compare(Long[] a, Long[] b) {
        throw new UnsupportedOperationException("不能进行compare操作");
    }

    @Override
    public int getMaxByteSize(Long[] value) {
        return (value.length * 8) + 4;
    }
}
