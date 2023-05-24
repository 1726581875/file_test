package com.moyu.test.store.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class BTreeMap<K extends Comparable, V> {

    private int maxNodeNum = 48;

    private TreeNode<K, V> rootNode;

    public BTreeMap() {
        this.rootNode = new TreeNode<>(this,  new ArrayList<>(), new ArrayList<>(), new ArrayList<>() , true);
    }

    public BTreeMap(int  maxNodeNum) {
        this.rootNode = new TreeNode<>(this,  new ArrayList<>(), new ArrayList<>(), new ArrayList<>() , true);
        this.maxNodeNum = maxNodeNum;
    }

    public V get(K k){
        return TreeNode.get(rootNode, k);
    }

    /**
     * 参考H2database关于b+树的实现，写得非常好的一段代码
     * 也删减不必要的程序，使其更加简单
     * @param key
     * @param value
     */
    private void put(K key, V value) {
        // 从根节点往下找，并且记录查找路径
        CursorPos<K,V> cursor = CursorPos.traverseDown(rootNode, key);
        int index = cursor.getIndex();
        TreeNode<K, V> node = cursor.getTreeNode();

        cursor = cursor.getParent();
        // index < 0表示找不到关键字,-1表示比任何关键字要小。其他数值计算通过(-index - 1)可以知道要插入的位置
        if (index < 0) {
            // 插入到叶子节点，b树每次插入都是在叶子节点插入
            node.insertLeaf(-index - 1, key, value);
            int keyCount;
            // 判断b树节点是否要分裂，当结点数大于最大节点树。关键子和值从中间分裂为两个节点，并会把中间关键字提取到父节点
            while ((keyCount = node.getKeyCount()) > maxNodeNum) {
                // 相当于除2
                int at = keyCount >> 1;
                //获取中间关键字
                K k = node.getKey(at);
                // 从中间分裂出新节点
                TreeNode<K, V> split = node.split(at);
                // 如果父节点是空，表示当前就是根节点, 新建新节点作为父节点，原先分裂的两个节点作为其子节点
                if (cursor == null) {
                    // 设置关键字
                    List<K> keys = new ArrayList<>();
                    keys.add(k);
                    // 设置子节点
                    List<TreeNode<K, V>> children = new ArrayList<>(2);
                    children.add(node);
                    children.add(split);
                    rootNode = new TreeNode<>(this, keys, null, children, false);
                    // 结束
                    break;
                }

                // 当前节点赋值给c
                TreeNode<K, V> c = node;
                // 赋值为父节点
                node = cursor.getTreeNode();
                // 拿到父节点中的插入位置
                index = cursor.getIndex();
                // 赋值为父节点, 往上，一直保持是node的父节点位置
                cursor = cursor.getParent();
                // 重新设置前面分裂左节点
                node.setChild(index, c);
                // 插入前面分裂的右节点
                node.insertNode(index + 1, k, split);

            }
        } else {
            // index > 0 在叶子节点找到对应关键字， 直接替换为新值
            node.setValue(index, value);
        }
    }


    public TreeNode<K, V> getRootNode() {
        return rootNode;
    }

    public static void main(String[] args) {
        BTreeMap<Integer,String> bTreeMap = new BTreeMap<>(5);
        //bTreeMap.put(0, "0");
        bTreeMap.put(1, "1");
        bTreeMap.put(2, "2");
        bTreeMap.put(3, "3");
        bTreeMap.put(4, "4");
        bTreeMap.put(5, "5");
        bTreeMap.put(6, "6");
        bTreeMap.put(7, "7");

        bTreeMap.put(8, "8");
        bTreeMap.put(9, "9");
        bTreeMap.put(10, "10");
        bTreeMap.put(11, "11");
        bTreeMap.put(12, "12");

        TreeNode<Integer, String> rootNode = bTreeMap.getRootNode();
        System.out.println(rootNode.toString());

        TreeNode<Integer, String> node0 = rootNode.getChildNodeList().get(0);
        System.out.println(node0.toString());

        TreeNode<Integer, String> node1 = rootNode.getChildNodeList().get(1);
        System.out.println(node1.toString());


        String s = bTreeMap.get(13);
        System.out.println("get result=" + s);
    }


}
