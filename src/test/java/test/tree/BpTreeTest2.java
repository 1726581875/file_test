package test.tree;

import com.moyu.test.store.data.tree.BTreeMap;
import com.moyu.test.store.data.tree.BpTreeStore;
import com.moyu.test.store.data.tree.Page;
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
            BTreeMap<Integer, String> bTreeMap = null;
            bTreeMap = new BTreeMap<>(5,
                    new IntColumnType(),
                    new StringColumnType(),
                    bpTreeStore);
            bTreeMap.initRootNode();

/*            for (int i = 0; i < 6; i++) {
                bTreeMap.put(i, "v" + i);
            }*/



/*            Page<Integer, String> rootNode = bTreeMap.getRootNode();
            System.out.println(rootNode.toString());*/

            String s = bTreeMap.get(2);
            System.out.println("get result=" + s);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bpTreeStore.close();
        }
    }
}
