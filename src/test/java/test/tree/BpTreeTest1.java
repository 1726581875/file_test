package test.tree;
import test.tree.btree.BpTreeMap;
/**
 * @author xiaomingzhang
 * @date 2023/5/24
 */
public class BpTreeTest1 {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        BpTreeMap<String,String> bTreeMap = new BpTreeMap<>(1024);
        for (int i = 0; i < 10000000; i++) {
            bTreeMap.put(i + "", "v" + i);
        }
        long point1 = System.currentTimeMillis();
        System.out.println("插入耗时:" + (point1- startTime) / 1000 + "s");

        String result = bTreeMap.get("99999");
        System.out.println("get result=" + result);

        long point2 = System.currentTimeMillis();
        System.out.println("查询耗时:" + (point2- point1) + "ms");

        int level = bTreeMap.getLevel();
        System.out.println("level=" + level);
    }




}
