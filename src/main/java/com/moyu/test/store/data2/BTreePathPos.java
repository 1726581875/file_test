package com.moyu.test.store.data2;

/**
 * @author xiaomingzhang
 * @date 2023/6/30
 */
public class BTreePathPos<K,V> {


    /**
     * The page at the current level.
     */
    private Page<K,V> treeNode;

    /**
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    private int index;

    /**
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    private BTreePathPos<K,V> parent;


    public BTreePathPos(Page<K,V> treeNode, int index, BTreePathPos<K,V> parent) {
        this.treeNode = treeNode;
        this.index = index;
        this.parent = parent;
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param page      root of the tree
     * @param key       the key to search for
     * @return head of the CursorPos chain (insertion point)
     */
    static <K,V> BTreePathPos<K,V> traverseDown(Page<K,V> page, K key) {
        BTreePathPos<K,V> cursorPos = null;
        while (!page.isLeaf()) {
            int index = page.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }
            cursorPos = new BTreePathPos<>(page, index, cursorPos);
            page = page.getMap().getChildPage(page, index);
        }
        return new BTreePathPos<>(page, page.binarySearch(key), cursorPos);
    }


    public Page<K, V> getTreeNode() {
        return treeNode;
    }

    public void setTreeNode(Page<K, V> treeNode) {
        this.treeNode = treeNode;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public BTreePathPos<K, V> getParent() {
        return parent;
    }

    public void setParent(BTreePathPos<K, V> parent) {
        this.parent = parent;
    }

    @Override
    public String toString() {
        return "CursorPos{" +
                "page=" + treeNode +
                ", index=" + index +
                ", parent=" + parent +
                '}';
    }

}
