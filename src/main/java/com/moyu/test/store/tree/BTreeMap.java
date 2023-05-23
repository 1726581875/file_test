package com.moyu.test.store.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class BTreeMap<K extends Comparable, V> {

    private int maxNodeNum;

    private TreeNode<K, V> rootNode;

    public BTreeMap() {
        this.rootNode = new TreeNode<>(5, true);
    }


    private void put(K key, V value) {

        CursorPos<K,V> pos = CursorPos.traverseDown(rootNode, key);
        int index = pos.getIndex();
        TreeNode<K, V> node = pos.getTreeNode();

        pos = pos.getParent();
        // index < 0表示找不到关键字,-1表示比任何关键字要小。其他数值计算通过(-index - 1)可以知道要插入的位置
        if (index < 0) {
            // 插入到叶子节点，
            node.insertLeaf(-index - 1, key, value);
            int keyCount;
            // 判断b树节点是否要分裂，主键数大于每页键数 或者页内空间不够并且叶子节点键数大于1（非叶子键数大于2）
            while ((keyCount = node.getKeyCount()) > maxNodeNum) {
                // 相当于除2
                int at = keyCount >> 1;
                //获取中间关键字
                K k = node.getKey(at);
                // 从中间分裂出新节点
                TreeNode<K, V> split = node.split(at);
                // 如果父节点是空，表示当前就是根节点, 新建新节点作为父节点，原先分裂的两个节点作为其子节点
                if (pos == null) {
                    // 设置关键字
                    List<K> keys = new ArrayList<>();
                    keys.add(k);
                    // 设置子节点
                    List<TreeNode<K, V>> children = new ArrayList<>(2);
                    children.add(node);
                    children.add(split);
                    node = new TreeNode<>(this, keys, children, false);
                    // 结束
                    break;
                }

                // 当前节点赋值给c
                TreeNode<K, V> c = node;
                // 赋值给父节点
                node = pos.getTreeNode();
                // 父节点中插入位置
                index = pos.getIndex();
                // pos赋值为父节点的父节点
                pos = pos.getParent();
                // 父节点重新设置当前节点(已经分裂)
                node.setChild(index, c);
                // 父节点插入新的分裂节点
                node.insertNode(index + 1, k, c);
            }
        } else {
            // index > 0 在叶子节点找到对应关键字， 直接替换为新值
            node.setValue(index, value);
        }
    }

    public void get() {

    }


}
