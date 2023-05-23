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
     * 树最大节点数
     */
    private int maxNodeNum;
    /**
     * 父节点
     */
    private TreeNode<K,V> parentNode;
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

    public TreeNode(int maxNodeNum, boolean isLeaf) {
        this.maxNodeNum = maxNodeNum;
        this.isLeaf = isLeaf;
        this.keywordList = new ArrayList<>();
        this.childNodeList = new ArrayList<>();
    }

    public TreeNode(TreeNode<K,V> parentNode,
                    List<K> keywordList,
                    List<TreeNode<K,V>> childNodeList,
                    boolean isLeaf) {
        this.parentNode = parentNode;
        this.keywordList = keywordList;
        this.childNodeList = childNodeList;
        this.isLeaf = isLeaf;
    }

    public TreeNode(BTreeMap<K,V> map,
                    List<K> keywordList,
                    List<TreeNode<K,V>> childNodeList,
                    boolean isLeaf) {
        this.map = map;
        this.keywordList = keywordList;
        this.childNodeList = childNodeList;
        this.isLeaf = isLeaf;
    }





    public void addKeyWord(K value) {
        if(this.keywordList.size() == 0) {
            this.keywordList.add(value);
        }
        for (int i = 0; i < this.keywordList.size(); i++) {
            K keyWord = this.keywordList.get(i);
            if(value.compareTo(keyWord) <= 0) {
                this.keywordList.add(i, value);
            }
        }
    }


    public void insertLeaf(int index, K key, V value){

    }

    public int getKeyCount() {
        return keywordList.size();
    }


    public K getKey(int index) {
        return keywordList.get(index);
    }


    public TreeNode<K,V> split(int index) {
        List<K> leftKeyWords = this.keywordList.subList(0, index);
        List<K> rightKeyWords = this.keywordList.subList(index + 1, keywordList.size());
        return null;
    }


    public void setChild(int index, TreeNode<K, V> node) {
        this.childNodeList.set(index, node);
    }

    public void insertNode(int index, K key, TreeNode<K, V> node) {
        this.keywordList.add(index,key);
        this.childNodeList.add(index, node);
    }

    public void setValue(int index, V value) {
        this.valueList.set(index, value);
    }


    public TreeNode<K,V> get(K value) {
        if(keywordList == null) {
            keywordList = new ArrayList<>();
        }


        keywordList.add(value);
        // 超出最大节点数，需要把中间节点提取到父节点
        if(keywordList.size() >= maxNodeNum) {

        }


        return this;
    }


    public int binarySearch(K key){

        return 0;
    }

    public int getMaxNodeNum() {
        return maxNodeNum;
    }

    public void setMaxNodeNum(int maxNodeNum) {
        this.maxNodeNum = maxNodeNum;
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

    public TreeNode<K, V> getParentNode() {
        return parentNode;
    }

    public void setParentNode(TreeNode<K, V> parentNode) {
        this.parentNode = parentNode;
    }

    public List<V> getValueList() {
        return valueList;
    }

    public void setValueList(List<V> valueList) {
        this.valueList = valueList;
    }




}
