package com.moyu.test.store.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class TreeNode<K extends Comparable, V> {

    private BTreeMap<K,V> map;
    /**
     * 节点关键词列表
     */
    private List<K> keywordList;

    /**
     * 树子节点列表
     */
    private List<TreeNode<K,V>> childNodeList;

    /**
     * 是否是叶子节点
     */
    private boolean isLeaf;
    /**
     *值列表
     */
    private List<V> valueList;


    public TreeNode(BTreeMap<K,V> map,
                    List<K> keywordList,
                    List<V> valueList,
                    List<TreeNode<K,V>> childNodeList,
                    boolean isLeaf) {
        this.map = map;
        this.keywordList = keywordList;
        this.valueList = valueList;
        this.childNodeList = childNodeList;
        this.isLeaf = isLeaf;
    }




    public static <K extends Comparable,V> V get(TreeNode<K,V> p, K key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValueList().get(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildNodeList().get(index);
        }
    }


    public void insertLeaf(int index, K key, V value) {
        if(index == this.keywordList.size()) {
            this.keywordList.add(key);
            this.valueList.add(value);
        } else {
            this.keywordList.add(index, key);
            this.valueList.add(index, value);
        }
    }

    public int getKeyCount() {
        return this.keywordList.size();
    }


    public K getKey(int index) {
        return this.keywordList.get(index);
    }




    public TreeNode<K, V> split(int index) {

        // 叶子节点,叶子节点的关键字数和值数量一样
        if(isLeaf) {
            // 分裂关键字
            List<K> rightKeywords =  splitLeafKey(index);;
            // 分裂值
            List<V> leftValues = new ArrayList<>(index);
            List<V> rightValues = new ArrayList<>(this.valueList.size() - index);
            for (int i = 0; i < this.valueList.size(); i++) {
                V value = this.valueList.get(i);
                if (i < index) {
                    leftValues.add(value);
                } else {
                    rightValues.add(value);
                }
            }
            this.valueList = leftValues;
            return createLeafNode(rightKeywords, rightValues);
            // 非叶子节点，关键字数比孩子节点数小1
        } else {
            // 分裂关键字
            List<K> leftKeywords = new ArrayList<>(index);
            List<K> rightKeywords = new ArrayList<>(this.keywordList.size() - index);
            for (int i = 0; i < this.keywordList.size(); i++) {
                K key = this.keywordList.get(i);
                // 中间之前,加到左边节点
                if (i < index) {
                    leftKeywords.add(key);
                    // 中间之后，加到右边节点
                } else if (i > index) {
                    rightKeywords.add(key);
                } else {
                    // 跳过中间关键字
                    continue;
                }
            }
            this.keywordList = leftKeywords;
            // 分裂孩子节点
            List<TreeNode<K,V>> leftChildNodes = new ArrayList<>(index);
            List<TreeNode<K,V>> rightChildNodes = new ArrayList<>(this.childNodeList.size() - index);
            for (int i = 0; i < this.childNodeList.size(); i++) {
                TreeNode<K,V> node = this.childNodeList.get(i);
                if (i < index + 1) {
                    leftChildNodes.add(node);
                } else {
                    rightChildNodes.add(node);
                }
            }
            this.childNodeList = leftChildNodes;

            return createNonLeafNode(rightKeywords, rightChildNodes);
        }
    }


    private TreeNode<K, V> createLeafNode(List<K> keywordList, List<V> valueList) {
        return new TreeNode<>(map, keywordList, valueList, null, true);
    }


    private TreeNode<K, V> createNonLeafNode(List<K> keywordList, List<TreeNode<K,V>> childNodeList) {
        return new TreeNode<>(map, keywordList,null, childNodeList, false);
    }


    private List<K> splitLeafKey(int index) {
        List<K> leftKeywords = new ArrayList<>(index);
        List<K> rightKeywords = new ArrayList<>(this.keywordList.size() - index);
        for (int i = 0; i < this.keywordList.size(); i++) {
            K key = this.keywordList.get(i);
            if (i < index) {
                leftKeywords.add(key);
            } else {
                rightKeywords.add(key);
            }
        }
        this.keywordList = leftKeywords;
        return rightKeywords;
    }




    public void setChild(int index, TreeNode<K, V> node) {
        this.childNodeList.set(index, node);
    }

    public void insertNode(int index, K key, TreeNode<K, V> node) {
        if (index >= this.keywordList.size()) {
            this.keywordList.add(key);
        } else {
            this.keywordList.add(index, key);
        }

        if (index >= this.childNodeList.size()) {
            this.childNodeList.add(node);
        } else {
            this.childNodeList.add(index, node);
        }
    }

    public void setValue(int index, V value) {
        this.valueList.set(index, value);
    }


    /**
     * 返回-1，表示比所有关键词都要小
     * 返回负数，表示找不到关键字，返回值为关键字插入位置
     * 返回正数，表示找到一样的关键字，其下标为返回值
     *
     *
     * @param key
     * @return
     */
    public int binarySearch(K key) {

        if(this.keywordList.size() == 0 || key.compareTo(this.keywordList.get(0)) < 0) {
            return -1;
        }
        for (int i = 0; i < this.keywordList.size(); i++) {
            K currKey = this.keywordList.get(i);
            if(currKey.compareTo(key) == 0) {
                return i;
                // this.keywordList是从小到大，当找到第一个比key大的值，当前的i就是插入位置
            } else if(currKey.compareTo(key) > 0) {
                return -(i + 1);
            }
        }
        return -(this.keywordList.size() + 1);
    }

    public List<K> getKeywordList() {
        return keywordList;
    }

    public void setKeywordList(List<K> keywordList) {
        this.keywordList = keywordList;
    }

    public List<TreeNode<K,V>> getChildNodeList() {
        return childNodeList;
    }

    public void setChildNodeList(List<TreeNode<K,V>> childNodeList) {
        this.childNodeList = childNodeList;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }


    public List<V> getValueList() {
        return valueList;
    }

    public void setValueList(List<V> valueList) {
        this.valueList = valueList;
    }


    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("");
        appendString(0, this, stringBuilder);

        return stringBuilder.toString();
    }

    private void appendString(Integer parent, TreeNode<K, V> node, StringBuilder stringBuilder) {
        if (node.isLeaf) {
            appendToString(parent, -1, node, stringBuilder);
        } else {
            appendToString(parent, 0, node, stringBuilder);
            List<TreeNode<K, V>> childNodeList = node.getChildNodeList();
            Integer p = parent + 1;
            for (int i = 0; i < childNodeList.size(); i++) {
                TreeNode<K, V> treeNode = childNodeList.get(i);
                appendToString(p, i, treeNode, stringBuilder);
                //Integer p2 = parent + 1;
                //appendString(p2, treeNode, stringBuilder);
            }
        }

    }


    private void appendToString(Integer parent, int index, TreeNode<K, V> node, StringBuilder stringBuilder) {
        stringBuilder.append("[" + parent +"]:" + index + ">>");
        if(node.isLeaf()) {
            stringBuilder.append("[leaf] "+"keys=" +node.getKeywordList() + "    values=" + node.getValueList());
        } else {
            stringBuilder.append("[nonLeaf] "+"keys=" +node.getKeywordList());
        }
        stringBuilder.append("\n");
    }
}
