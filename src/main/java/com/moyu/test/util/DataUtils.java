package com.moyu.test.util;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/23
 */
public class DataUtils {

    /**
     * 参考H2database的DataUtils.writeStringData类
     * @param buff
     * @param s
     * @param len
     */
    public static void writeStringData(ByteBuffer buff, String s, int len) {
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            // 0x80十进制为128，二进制10000000
            if (c < 0x80) {
                /**
                 * a字符转int后的二进制序列
                 * 00000000 00000000 00000000 1100001
                 * 转byte只保留了1100001
                 */
                // 小于128，一个字节可以存储，也就是常见的ASCII字符
                buff.put((byte) c);
                // 十进制2048，二进制00001000 00000000
            } else if (c >= 0x800) {
                /**
                 * 字符 "啊" int为 21834
                 * 00000000 00000000 01010101 01001010
                 *
                 * 00000000 0000[0000 0101] | 11100000 = 111[00101]
                 * 00000000 00000000 01[010101 01] & 00111111 = 00[010101]
                 * 00000000 00000000 01010101 01001010 & 00111111 = 00[001010]
                 */
                // 大于等于2048拆分为3个字节存储
                // 0xe0=11100000
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else {
                // 大于等于128，并且小于2048，按两个字节存储
                // 0xc0二进制为11000000,
                buff.put((byte) (0xc0 | (c >> 6)));
                //只保留后六位(0x3f十进制是63，二进制是00111111。)
                buff.put((byte) (c & 0x3f));
            }
        }
    }


    /**
     * 参考H2database的DataUtils.readString类
     * @param buff
     * @param len
     * @return
     */
    public static String readString(ByteBuffer buff, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            // & 0xff是保持二进制补码的一致性。
            /**
             * 将byte 类型提升为int时候，b的补码提升为 32位，补码的高位补1
             * byte b = -127 二进制为10000001
             * byte->int 计算机背后存储的二进制补码由 10000001（8位）转化成了 11111111 11111111 11111111 10000001
             *
             * 为了保持原始的二进制序列 & 0xff
             * 11111111 11111111 11111111 10000001 & 00000000 00000000 00000000 11111111 = 00000000 00000000 00000000 10000001
             */
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;

                // 0xe0=11100000
            } else if (x >= 0xe0) {
                /**
                 * 字符 "啊" int为 21834
                 * 00000000 00000000 01010101 01001010
                 *
                 * 转换为3个字节
                 * 00000000 0000[0000 0101] | 11100000 = 111[00101]
                 * 00000000 00000000 01[010101 01] & 00111111 = 00[010101]
                 * 00000000 00000000 01010101 01001010 & 00111111 = 00[001010]
                 *
                 * 3字节再转换为java字符
                 * 111[00101] | 00001111 = 0000[0101] << 12 = 00000000 00000000 [0101]0000 00000000
                 * 00[010101] & 00111111 = 00[010101] << 6  = 00000000 00000000 0000 [0101 01]000000
                 * 00[001010] & 00111111 = 00[010101]       = 00000000 00000000 00000000  00 [001010]
                 *
                 *   00000000 00000000 01010000 00000000
                 * + 00000000 00000000 00000101 01000000
                 * + 00000000 00000000 00000000 00001010
                 * = 00000000 00000000 01010101 01001010
                 */
                chars[i] = (char) (((x & 0xf) << 12)
                        + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
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
        String str = "啊";
        int length = str.length();

        Character c1 = 'a';

        int c = str.charAt(0);
        // 0x80十进制为128，二进制10000000
        if (c < 0x80) {
            System.out.println("1");
        } else if (c >= 0x800) {
            System.out.println("byte1:" + (int)(((byte) (0xe0 | (c >> 12)))));
            System.out.println("byte2:" + (int)((byte) (((c >> 6) & 0x3f))));
            System.out.println(("byte3:" + (int)(byte) (c & 0x3f)));
        } else {
            System.out.println("3");
            System.out.println(length);
            System.out.println((int) str.charAt(0));
        }
    }


}
