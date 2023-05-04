package test;

import com.moyu.test.store.Chunk;
import com.moyu.test.store.UnfixedLengthStore;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class UnfixedLengthStoreTest {

    public static void main(String[] args) throws IOException {
        testWriteChunk();
        testReadChunk();
    }


    private static void testReadChunk() {
        UnfixedLengthStore unfixedLengthStore = null;
        try {
            String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
            unfixedLengthStore = new UnfixedLengthStore(filePath);
            Chunk nextChunk = null;
            while ((nextChunk = unfixedLengthStore.getNextChunk()) != null) {
                System.out.println(nextChunk);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (unfixedLengthStore != null) {
                unfixedLengthStore.close();
            }
        }

    }


    private static void testWriteChunk() {
        UnfixedLengthStore unfixedLengthStore = null;
        try {
            String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
            unfixedLengthStore = new UnfixedLengthStore(filePath);
            unfixedLengthStore.writeData("摸鱼啊啊啊");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(unfixedLengthStore != null) {
                unfixedLengthStore.close();
            }
        }

    }

}
