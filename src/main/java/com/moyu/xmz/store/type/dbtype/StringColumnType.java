package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.store.common.WriteBuffer;
import com.moyu.xmz.common.util.DataUtils;

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

    @Override
    public Class<?> getValueTypeClass() {
        return String.class;
    }

    @Override
    public int getMaxByteLen(String value) {
        return value.length() * 3 + 4;
    }

}
