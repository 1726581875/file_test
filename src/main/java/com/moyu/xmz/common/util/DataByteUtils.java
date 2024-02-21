package com.moyu.xmz.common.util;

import com.moyu.xmz.store.accessor.FileAccessor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/23
 */
public class DataByteUtils {

    /**
     * 单个字符按照UTF-8编码规则转为字节
     * 2 字节编码：Unicode 码点范围 U+0080 到 U+07FF，编码格式为 110xxxxx 10xxxxxx。
     * 3 字节编码：Unicode 码点范围 U+0800 到 U+FFFF，编码格式为 1110xxxx 10xxxxxx 10xxxxxx。
     * @param buff
     * @param s
     * @param len
     */
    public static void writeStringData(ByteBuffer buff, String s, int len) {
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            // 0x80十进制为128，二进制10000000
            if (c < 0x80) {
                // 小于128，一个字节可以存储，也就是常见的ASCII字符
                buff.put((byte) c);
                // 0x800十进制2048，二进制00001000 00000000
            } else if (c < 0x800) {
                // 大于等于128，并且小于2048，按两个字节存储
                buff.put((byte) (0b11000000 | (c >> 6)));
                //只保留后六位(0x3f十进制是63，二进制是00111111。)
                buff.put((byte) (0b10000000 | (c & 0b00111111)));
            } else if (c >= 0x800) {
                // 大于等于2048拆分为3个字节存储
                buff.put((byte) (0b11100000 | (c >> 12)));
                buff.put((byte) (0b10000000 | (c >> 6 & 0b00111111)));
                buff.put((byte) (0b10000000 | (c & 0b00111111)));
            }
        }
    }


    /**
     * 字节转字符串
     * @param buff
     * @param len
     * @return
     */
    public static String readString(ByteBuffer buff, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            // 类型提升为int时0xff保持二进制补码的一致性
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if(x < 0b11100000) {
                int first = (x & 0b00011111) << 6;
                int second = (buff.get() & 0b00111111);
                chars[i] = (char) (first + second);
            } else if (x >= 0b11100000) {
                int first = (x & 0b00001111) << 12;
                int second = (buff.get() & 0b00111111) << 6;
                int third = buff.get() & 0b00111111;
                chars[i] = (char) (first + second + third);
            }
        }
        return new String(chars);
    }


    public static int getDateStringByteLength(String dataStr) {
        int len = 0;
        for (int i = 0; i < dataStr.length(); i++) {
            int c = dataStr.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c >= 0x800) {
                len += 3;
            } else {
                len += 2;
            }
        }
        return len;
    }




    /**
     * int转byte存储
     * @param buff
     * @param value
     */
    public static void writeInt(ByteBuffer buff, int value) {
        buff.put((byte) (value >> 24 & 0xff));
        buff.put((byte) (value >> 16 & 0xff));
        buff.put((byte) (value >> 8 & 0xff));
        buff.put((byte) (value & 0xff));
    }

    public static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >> 24 & 0xff),
                (byte) (value >> 16 & 0xff),
                (byte) (value >> 8 & 0xff),
                (byte) (value & 0xff)
        };
    }

    /**
     * byte转int
     * @param buff
     * @return
     */
    public static int readInt(ByteBuffer buff) {
        int i1 = (buff.get() & 0xff) << 24;
        int i2 = (buff.get() & 0xff) << 16;
        int i3 = (buff.get() & 0xff) << 8;
        int i4 = (buff.get() & 0xff);
        return i1 + i2 + i3 + i4;
    }


    /**
     * long转byte存储
     *
     * 注：方法代码相当于，只是做了下取巧简化
     *   buff.put((byte) (value >> 56 & 0xff));
     *   buff.put((byte) (value >> 48 & 0xff));
     *   buff.put((byte) (value >> 40 & 0xff));
     *   buff.put((byte) (value >> 32 & 0xff));
     *   buff.put((byte) (value >> 24 & 0xff));
     *   buff.put((byte) (value >> 16 & 0xff));
     *   buff.put((byte) (value >> 8 & 0xff));
     *   buff.put((byte) (value & 0xff));
     *
     * @param buff
     * @param value
     */
    public static void writeLong(ByteBuffer buff, long value) {
        // long字长度8
        int i = 8;
        while (i > 0) {
            i = i - 1;
            buff.put((byte) ((value >> (i << 3)) & 0xff));
        }
    }

    /**
     * byte转long
     * @param buff
     * @return
     */
    public static long readLong(ByteBuffer buff) {
        long result = 0;
        int i = 8;
        while (i > 0) {
            i = i - 1;
            result |=  ((long) buff.get() & 0xff) << (i << 3);
        }
        return result;
    }





    public static void main(String[] args) {
        String str = "\uD83D\uDE00";
        try {
            FileAccessor fileAccessor = new FileAccessor("D:\\tmp\\1.txt");
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            DataByteUtils.writeStringData(byteBuffer, str, str.length());
            byteBuffer.flip();
            fileAccessor.write(byteBuffer, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer bf = ByteBuffer.allocate(1024);
        DataByteUtils.writeStringData(bf, str, str.length());
        bf.flip();

        System.out.println(DataByteUtils.readString(bf, str.length()));
    }


}
