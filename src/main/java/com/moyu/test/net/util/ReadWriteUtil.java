package com.moyu.test.net.util;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class ReadWriteUtil {

    public static void writeString(WriteBuffer writeBuffer, String str) {
        if (str == null) {
            writeBuffer.putInt(-1);
        } else if (str.length() == 0) {
            writeBuffer.putInt(str.length());
        } else {
            writeBuffer.putInt(str.length());
            writeBuffer.putStringData(str, str.length());
        }
    }


    public static String readString(ByteBuffer byteBuffer) {
        int len = byteBuffer.getInt();
        if (len == -1) {
            return null;
        } else if (len == 0) {
            return "";
        } else {
            return DataUtils.readString(byteBuffer, len);
        }
    }


}
