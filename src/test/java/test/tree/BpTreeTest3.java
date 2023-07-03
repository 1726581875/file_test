package test.tree;

import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.BTreeStore;
import com.moyu.test.store.type.IntColumnType;
import com.moyu.test.store.type.StringColumnType;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/5/25
 */
public class BpTreeTest3 {

    private static final String testPath = PathUtil.getBaseDirPath() + File.separator + "b3.data";
    /**
     * 写入磁盘测试
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        FileUtil.deleteOnExists(testPath);

        BTreeStore bpTreeStore = new BTreeStore(testPath);
        try {
            BTreeMap<Integer, String> bTreeMap = null;
            bTreeMap = new BTreeMap<>(new IntColumnType(),
                    new StringColumnType(),
                    bpTreeStore, false);
            bTreeMap.initRootNode();

            int count = 100000;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                bTreeMap.put(i, "v" + i);
            }

            //bTreeMap.commitSaveDisk();

            long point1 = System.currentTimeMillis();
            System.out.println("存储" + count + "条数据，耗时:" + (point1 - startTime) / 1000 + "s");



            String s = bTreeMap.get(9999);
            long point2 = System.currentTimeMillis();
            System.out.println("get result=" + s);
            System.out.println("查询耗时:" + (point2 - point1) + "ms");



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bpTreeStore.close();
        }
    }
}
