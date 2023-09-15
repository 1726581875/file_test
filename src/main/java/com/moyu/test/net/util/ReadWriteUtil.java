package com.moyu.test.net.util;

import com.moyu.test.store.WriteBuffer;
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

    public static void writeString(WriteBuffer writeBuffer, String str) {
        if (str == null) {
            writeBuffer.putInt(-1);
        } else if (str.length() == 0) {
            byte[] bytes = str.getBytes();
            writeBuffer.putInt(bytes.length);
        } else {
            byte[] bytes = str.getBytes();
            writeBuffer.putInt(bytes.length);
            writeBuffer.put(bytes);
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
            return new String(bytes);
        }
    }


    public static String readString(DataInputStream in) throws IOException {
        Integer byteLen = in.readInt();
        byte[] bytes = new byte[byteLen];
        for (int i = 0; i < byteLen; i++) {
            bytes[i] = in.readByte();
        }
        return new String(bytes);
    }

    public static String writeString(DataOutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
        return new String(bytes);
    }


}
