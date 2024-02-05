package test.tree;

import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.tree.BTreeStore;
import com.moyu.xmz.store.type.dbtype.IntType;
import com.moyu.xmz.store.type.dbtype.StringType;
import com.moyu.xmz.common.util.FileUtil;
import com.moyu.xmz.common.util.PathUtil;

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
            bTreeMap = new BTreeMap<>(new IntType(),
                    new StringType(),
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
