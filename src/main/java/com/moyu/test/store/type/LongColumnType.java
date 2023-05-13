package com.moyu.test.store.type;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class LongColumnType extends AbstractColumnType<Long> {

    @Override
    protected Long readValue(ByteBuffer byteBuffer) {
        return DataUtils.readLong(byteBuffer);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Long value) {
        writeBuffer.putLong(value);
    }


}
