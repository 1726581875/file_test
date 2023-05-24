package test.tree;
import com.moyu.test.store.tree.BTreeMap;
/**
 * @author xiaomingzhang
 * @date 2023/5/24
 */
public class BtreeTest {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        BTreeMap<Integer,String> bTreeMap = new BTreeMap<>(1024);
        for (int i = 0; i < 10000000; i++) {
            bTreeMap.put(i, "v" + i);
        }
        long point1 = System.currentTimeMillis();
        System.out.println("插入耗时:" + (point1- startTime) / 1000 + "s");

        String result = bTreeMap.get(99999);
        System.out.println("get result=" + result);

        long point2 = System.currentTimeMillis();
        System.out.println("查询耗时:" + (point2- point1) + "ms");

        int level = bTreeMap.getLevel();
        System.out.println("level=" + level);
    }




}
