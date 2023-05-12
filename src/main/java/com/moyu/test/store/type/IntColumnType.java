package com.moyu.test.store.type;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class IntColumnType extends AbstractColumnType<Integer> {


    @Override
    protected void expand(ByteBuffer byteBuffer) {
        if(byteBuffer.remaining() < 4) {
            expand(byteBuffer, 4);
        }
    }

    @Override
    protected Integer readValue(ByteBuffer byteBuffer) {
        return DataUtils.readInt(byteBuffer);
    }

    @Override
    protected void writeValue(ByteBuffer byteBuffer, Integer value) {
        DataUtils.writeInt(byteBuffer, value);
    }

}
