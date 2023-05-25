/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package test.tree.btree;

/**
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * from a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public final class CursorPos<K extends Comparable,V> {

    /**
     * The page at the current level.
     */
    private TreeNode<K,V> treeNode;

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
    private CursorPos<K,V> parent;


    public CursorPos(TreeNode<K,V> treeNode, int index, CursorPos<K,V> parent) {
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
    static <K extends Comparable,V> CursorPos<K,V> traverseDown(TreeNode<K,V> page, K key) {
        CursorPos<K,V> cursorPos = null;
        while (!page.isLeaf()) {
            int index = page.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }
            cursorPos = new CursorPos<>(page, index, cursorPos);
            page = page.getChildNodeList().get(index);
        }
        return new CursorPos<>(page, page.binarySearch(key), cursorPos);
    }


    public TreeNode<K, V> getTreeNode() {
        return treeNode;
    }

    public void setTreeNode(TreeNode<K, V> treeNode) {
        this.treeNode = treeNode;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public CursorPos<K, V> getParent() {
        return parent;
    }

    public void setParent(CursorPos<K, V> parent) {
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

