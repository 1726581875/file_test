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
        UnfixedLengthStore unfixedLengthStore = null;
        try {
            String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
            unfixedLengthStore = new UnfixedLengthStore(filePath);
            Chunk nextChunk = unfixedLengthStore.getNextChunk();
            System.out.println(nextChunk);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(unfixedLengthStore != null) {
                unfixedLengthStore.close();
            }
        }
    }

}
