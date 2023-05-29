package test.tree;

import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.data.tree.BpTreeStore;
import com.moyu.test.store.type.IntColumnType;
import com.moyu.test.store.type.StringColumnType;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/5/25
 */
public class BpTreeTest2 {

    /**
     * 写入磁盘测试
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        BpTreeStore bpTreeStore = new BpTreeStore();
        try {
            BpTreeMap<Integer, String> bTreeMap = null;
            bTreeMap = new BpTreeMap<>(1024,
                    new IntColumnType(),
                    new StringColumnType(),
                    bpTreeStore);
            bTreeMap.initRootNode();

            int count = 1000000;
            long startTime = System.currentTimeMillis();
/*            for (int i = 0; i < count; i++) {
                bTreeMap.put(i, "v" + i);
            }*/
            long point1 = System.currentTimeMillis();
            System.out.println("存储" + count + "条数据，耗时:" + (point1 - startTime) / 1000 + "s");

            String s = bTreeMap.get(8888);

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
