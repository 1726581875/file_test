package test.utils;

import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/28
 */
public class DataUtilsTest {

    public static void main(String[] args) {
        testIntByteConvert();
        testLongByteConvert();
    }


    private static void testIntByteConvert() {
        ByteBuffer intBufferTest = ByteBuffer.allocate(4);
        DataUtils.writeInt(intBufferTest, 520);
        intBufferTest.rewind();
        int i = DataUtils.readInt(intBufferTest);
        System.out.println(i);
    }

    private static void testLongByteConvert() {
        ByteBuffer longBufferTest = ByteBuffer.allocate(8);
        DataUtils.writeLong(longBufferTest, 520025L);
        longBufferTest.rewind();
        long longResult = DataUtils.readLong(longBufferTest);
        System.out.println(longResult);
    }


}
