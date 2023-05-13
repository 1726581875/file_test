package com.moyu.test.store.type;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class StringColumnType extends AbstractColumnType<String> {


    @Override
    protected String readValue(ByteBuffer byteBuffer) {
        int charLen = DataUtils.readInt(byteBuffer);
        return DataUtils.readString(byteBuffer, charLen);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, String value) {
        writeBuffer.putInt(value.length());
        writeBuffer.putStringData(value, value.length());
    }

}
