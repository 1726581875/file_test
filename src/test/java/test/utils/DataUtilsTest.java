package test.utils;

import com.moyu.test.util.AssertUtil;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.Random;

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
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            int randomInt = random.nextInt(Integer.MAX_VALUE);
            ByteBuffer intBufferTest = ByteBuffer.allocate(4);
            DataUtils.writeInt(intBufferTest, randomInt);
            intBufferTest.rewind();
            int result = DataUtils.readInt(intBufferTest);
            System.out.println("int原值:" + randomInt + ",经过转换再恢复的值:" + result);
            AssertUtil.assertTrue(randomInt == result, "转换后的int的值必须和原值相等");
        }
    }

    private static void testLongByteConvert() {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            long randomLong = random.nextLong();
            ByteBuffer longBufferTest = ByteBuffer.allocate(8);
            DataUtils.writeLong(longBufferTest, randomLong);
            longBufferTest.rewind();
            long longResult = DataUtils.readLong(longBufferTest);
            System.out.println(randomLong);
            System.out.println("long原值:" + randomLong + ",经过转换再恢复的值:" + longResult);
            AssertUtil.assertTrue(randomLong == longResult, "转换后的long的值必须和原值相等");
        }

    }


}
