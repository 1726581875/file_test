package com.moyu.test.store.type;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class LongColumnType extends AbstractColumnType<Long> {

    @Override
    protected void expand(ByteBuffer byteBuffer) {
        if(byteBuffer.remaining() < 8) {
            expand(byteBuffer, 8);
        }
    }

    @Override
    protected Long readValue(ByteBuffer byteBuffer) {
        return DataUtils.readLong(byteBuffer);
    }

    @Override
    protected void writeValue(ByteBuffer byteBuffer, Long value) {
        DataUtils.writeLong(byteBuffer, value);
    }


}
