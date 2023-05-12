package test.data;

import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.DataRow;
import com.moyu.test.util.FileUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class DataChunkStoreTest {


    public static void main(String[] args) {
        testWriteRow();
    }






    private static void testWriteRow() {
        String filePath = "D:\\mytest\\fileTest\\test.d";

        FileUtil.deleteOnExists(filePath);

        DataChunkStore dataChunkStore = null;
        try {
            dataChunkStore = new DataChunkStore(filePath);
            // create a chunk
            dataChunkStore.createChunk();

            // write data
            for (int i = 0; i < 1024; i++) {
                try {
                    dataChunkStore.addRow("hello world!hello world!啊啊啊啊".getBytes());
                    System.out.println("currNum=" + i);
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            // print
            DataChunk chunk = dataChunkStore.getChunk(0);
            System.out.println(chunk);
            chunk.getDataRowList().forEach(row -> {
                System.out.println(row);
                System.out.println(new String(row.getRow()));
            });
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dataChunkStore != null) {
                dataChunkStore.close();
            }
        }
    }


}
