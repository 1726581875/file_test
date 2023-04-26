package com.moyu.test.util;

import com.moyu.test.store.FileStore;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/23
 * 参考H2database的DataUtils类
 */
public class DataUtils {

    private static String filePath = "D:\\mytest\\fileTest\\1.xmz";
    private static String filePath2 = "D:\\mytest\\fileTest\\2.xmz";


    public static void writeStringData(ByteBuffer buff, String s, int len) {
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            // 0x80十进制为128，二进制10000000
            if (c < 0x80) {
                // 小于128，一个字节可以存储，也就是常见的ASCII字符
                buff.put((byte) c);
                // 十进制2048，二进制100000000000
            } else if (c >= 0x800) {
                // 大于等于2048拆分为3个字节存储
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else { // 大于等于128，并且小于2048，按两个字节存储
                // 0xc0二进制为11000000
                buff.put((byte) (0xc0 | (c >> 6)));
                buff.put((byte) (c & 0x3f));
            }
        }
    }


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
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12)
                        + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
            }
        }
        return new String(chars);
    }


    public static void writeInt(ByteBuffer buff, int value) {
        buff.put((byte) (value >> 24 & 0xff));
        buff.put((byte) (value >> 16 & 0xff));
        buff.put((byte) (value >> 8 & 0xff));
        buff.put((byte) (value & 0xff));
    }

    public static int readInt(ByteBuffer buff) {
        int i1 = (buff.get() & 0xff) << 24;
        int i2 = (buff.get() & 0xff) << 16;
        int i3 = (buff.get() & 0xff) << 8;
        int i4 = (buff.get() & 0xff);
        return i1 + i2 + i3 + i4;
    }


    public static void main(String[] args) {

        ByteBuffer intBufferTest = ByteBuffer.allocate(4);
        writeInt(intBufferTest, 520);
        intBufferTest.rewind();
        int i = readInt(intBufferTest);
        System.out.println(i);
    }




    private static void writeTest1() {

        FileUtil.createFileIfNotExists(filePath);
        FileStore fileStore = null;
        try {
            fileStore = new FileStore(filePath);
            // 写文件
            String str = "Hello 啊World !";
            ByteBuffer byteBuffer = ByteBuffer.allocate(str.length() * 3);

            writeStringData(byteBuffer, str, str.length());
            byteBuffer.rewind();
            fileStore.write(byteBuffer, 0);

            // 读文件
            ByteBuffer readBuff = fileStore.read(0, str.length() * 3 - 1);
            System.out.println(readString(readBuff, str.length()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }


    private static void writeTest2() {

        FileUtil.createFileIfNotExists(filePath2);
        FileStore fileStore = null;
        try {
            fileStore = new FileStore(filePath2);
            // 写文件
            String str = "Hello 啊World !";
            ByteBuffer byteBuffer = ByteBuffer.wrap(str.getBytes());

            byteBuffer.rewind();
            fileStore.write(byteBuffer, 0);

            // 读文件
            ByteBuffer readBuff = fileStore.read(0, str.length());
            System.out.println(new String(readBuff.array()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }


}
