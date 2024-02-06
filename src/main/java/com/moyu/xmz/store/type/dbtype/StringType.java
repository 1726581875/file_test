package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.common.util.DataByteUtils;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class StringType extends AbstractDbType<String> {


    @Override
    protected String readValue(ByteBuffer byteBuffer) {
        int charLen = DataByteUtils.readInt(byteBuffer);
        return DataByteUtils.readString(byteBuffer, charLen);
    }

    @Override
    protected void writeValue(DynByteBuffer buffer, String value) {
        buffer.putInt(value.length());
        buffer.putString(value);
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
