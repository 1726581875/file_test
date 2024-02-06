package com.moyu.xmz.net.util;

import com.moyu.xmz.common.DynByteBuffer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class ReadWriteUtil {

    public static void writeString(DynByteBuffer dynByteBuffer, String str) {
        if (str == null) {
            dynByteBuffer.putInt(-1);
        } else if (str.length() == 0) {
            byte[] bytes = str.getBytes();
            dynByteBuffer.putInt(bytes.length);
        } else {
            byte[] bytes = str.getBytes(Charset.forName("UTF-8"));
            dynByteBuffer.putInt(bytes.length);
            dynByteBuffer.put(bytes);
        }
    }


    public static String readString(ByteBuffer byteBuffer) {
        int len = byteBuffer.getInt();
        if (len == -1) {
            return null;
        } else if (len == 0) {
            return "";
        } else {
            byte[] bytes = new byte[len];
            byteBuffer.get(bytes);
            return new String(bytes, Charset.forName("UTF-8"));
        }
    }


    public static String readString(DataInputStream in) throws IOException {
        Integer byteLen = in.readInt();
        byte[] bytes = new byte[byteLen];
        for (int i = 0; i < byteLen; i++) {
            bytes[i] = in.readByte();
        }
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static String writeString(DataOutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(Charset.forName("UTF-8"));
        out.writeInt(bytes.length);
        out.write(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }


}
