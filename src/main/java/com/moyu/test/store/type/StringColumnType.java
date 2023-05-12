package com.moyu.test.store.type;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class StringColumnType extends AbstractColumnType<String> {

    @Override
    protected void expand(ByteBuffer byteBuffer) {
    }

    @Override
    protected String readValue(ByteBuffer byteBuffer) {
        int charLen = DataUtils.readInt(byteBuffer);
        return DataUtils.readString(byteBuffer, charLen);
    }

    @Override
    protected void writeValue(ByteBuffer byteBuffer, String value) {
        int length = value.length();
        int maxCharLen = length * 3 + 4;
        if (byteBuffer.remaining() < maxCharLen) {
            expand(byteBuffer, maxCharLen);
        }
        DataUtils.writeInt(byteBuffer, length);
        DataUtils.writeStringData(byteBuffer, value, length);
    }

}
