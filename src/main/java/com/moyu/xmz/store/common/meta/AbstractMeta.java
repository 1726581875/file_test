package com.moyu.xmz.store.common.meta;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2024/1/29
 */
public abstract class AbstractMeta {


    protected void writeString(DynByteBuffer buffer, String str) {
        if (str == null) {
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) 1);
            buffer.putInt(str.length());
            buffer.putString(str);
        }
    }

    protected String readString(ByteBuffer byteBuffer) {
        byte flag = byteBuffer.get();
        if (flag == (byte) 0) {
            return null;
        } else {
            int len = DataByteUtils.readInt(byteBuffer);
            return DataByteUtils.readString(byteBuffer, len);
        }
    }

}
