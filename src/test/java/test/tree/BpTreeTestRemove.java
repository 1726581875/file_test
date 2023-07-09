package test.tree;

import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.data.tree.BpTreeStore;
import com.moyu.test.store.type.dbtype.IntColumnType;
import com.moyu.test.store.type.dbtype.StringColumnType;
import com.moyu.test.util.PathUtil;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/6/2
 */
public class BpTreeTestRemove {

    /**
     * 写入磁盘测试
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        BpTreeStore bpTreeStore = new BpTreeStore(PathUtil.getBaseDirPath() + "/" + "bp.d");
        try {
            BpTreeMap<Integer, String> bTreeMap = null;
            bTreeMap = new BpTreeMap<>(64, new IntColumnType(), new StringColumnType(),
                    bpTreeStore, true);
            bTreeMap.initRootNode();

            int count = 1000;
            long startTime = System.currentTimeMillis();
/*            for (int i = 0; i < count; i++) {
                bTreeMap.put(i, "v" + i);
            }*/

            //bTreeMap.commitSaveDisk();

            long point1 = System.currentTimeMillis();
            System.out.println("存储" + count + "条数据，耗时:" + (point1 - startTime) / 1000 + "s");


            //bTreeMap.remove(999);

            String s = bTreeMap.get(999);
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
