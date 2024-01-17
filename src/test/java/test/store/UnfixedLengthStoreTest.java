package test.store;

import test.readwrite.entity.Chunk;
import test.readwrite.UnfixedLengthStore;
import com.moyu.xmz.common.util.FileUtil;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class UnfixedLengthStoreTest {

    private static final String filePath = "D:\\mytest\\fileTest\\unfixLen_test.xmz";

    public static void main(String[] args) throws IOException {
        if (!FileUtil.exists(filePath)) {
            UnfixedLengthStore.createAndInitFile(filePath);
        }
        testWriteChunk();
        testReadChunk();
    }


    private static void testReadChunk() {
        UnfixedLengthStore unfixedLengthStore = null;
        try {
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
