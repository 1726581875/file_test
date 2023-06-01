package com.moyu.test.store.data.tree;

import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.*;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class BpTreeMap<K extends Comparable, V> {

    private DataType<K> keyType;

    private DataType<V> valueType;

    private BpTreeStore bpTreeStore;

    private int level;

    private int maxNodeNum = 48;

    private Page<K, V> rootNode;

    private boolean isAutoCommit;

    private int nextPageIndex;


    public BpTreeMap(int maxNodeNum,
                     DataType<K> keyType,
                     DataType<V> valueType,
                     BpTreeStore bpTreeStore) {
        this.maxNodeNum = maxNodeNum;
        this.keyType = keyType;
        this.valueType = valueType;
        this.bpTreeStore = bpTreeStore;
        this.isAutoCommit = true;
    }

    public BpTreeMap(int maxNodeNum,
                     DataType<K> keyType,
                     DataType<V> valueType,
                     BpTreeStore bpTreeStore, boolean isAutoCommit) {
        this.maxNodeNum = maxNodeNum;
        this.keyType = keyType;
        this.valueType = valueType;
        this.bpTreeStore = bpTreeStore;
        this.isAutoCommit = isAutoCommit;
    }



    public void initRootNode() {
        if (bpTreeStore.getPageCount() == 0) {
            this.rootNode = new Page<>(this, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), true, bpTreeStore.getNextPageIndex());
            bpTreeStore.savePage(rootNode);
            bpTreeStore.updateRootPos(rootNode.getStartPos());
        } else {
            this.rootNode = bpTreeStore.getRootPage(this);
        }
        this.nextPageIndex++;
    }


    public V get(K k) {
        Page<K, V> page = rootNode;
        while (true) {
            int index = page.binarySearch(k);
            if (page.isLeaf()) {
                return index >= 0 ? page.getValueList().get(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            page = getChildPage(page, index);
        }
    }

    public Page<K, V> getChildPage(Page<K, V> page, int index) {
        Page<K, V> childPage = null;
        if (page.getChildNodeList() == null || page.getChildNodeList().size() == 0) {
            // 从磁盘中获取
            Long childPos = page.getChildPosList().get(index);
            childPage = bpTreeStore.getPage(childPos, this);
        } else {
            childPage = page.getChildNodeList().get(index);
        }
        return childPage;
    }




    /**
     * 参考H2database关于b+树的实现，写得非常好的一段代码
     * 也删减不必要的程序，使其更加简单
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        // 从根节点往下找，并且记录查找路径
        CursorPos<K,V> cursor = CursorPos.traverseDown(rootNode, key);
        int index = cursor.getIndex();
        Page<K, V> node = cursor.getTreeNode();
        cursor = cursor.getParent();
        // index < 0表示找不到关键字,-1表示比任何关键字要小。其他数值计算通过(-index - 1)可以知道要插入的位置
        if (index < 0) {
            // 插入到叶子节点，b树每次插入都是在叶子节点插入
            node.insertLeaf(-index - 1, key, value);
            int keyCount;
            // 判断b树节点是否要分裂，当结点数大于最大节点树。关键子和值从中间分裂为两个节点，并会把中间关键字提取到父节点
            while ((keyCount = node.getKeywordCount()) > maxNodeNum
                    || node.getCurrMaxByteLen() > Page.PAGE_SIZE) {
                // 相当于除2
                int at = keyCount >> 1;
                //获取中间关键字
                K k = node.getKey(at);
                // 从中间分裂出新节点
                Page<K, V> split = node.split(at);
                // 如果父节点是空，表示当前就是根节点, 新建新节点作为父节点，原先分裂的两个节点作为其子节点
                if (cursor == null) {
                    // 设置关键字
                    List<K> keys = new ArrayList<>();
                    keys.add(k);
                    // 设置子节点
                    List<Page<K, V>> children = new ArrayList<>(2);
                    children.add(node);
                    children.add(split);
                    int nextPageIndex = bpTreeStore.getNextPageIndex();
                    rootNode = new Page<>(this, keys, null, children, false, nextPageIndex);
                    level++;

                    // 保存新的根节点到磁盘
                    bpTreeStore.savePage(rootNode);
                    bpTreeStore.updateRootPos(rootNode.getStartPos());
                    // 保存左边节点和右边节点
                    bpTreeStore.savePage(split);
                    bpTreeStore.savePage(node);
                    // 结束
                    break;
                }

                // 当前节点赋值给c
                Page<K, V> c = node;
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

                // 更新到磁盘
                bpTreeStore.savePage(c);
                bpTreeStore.savePage(split);
            }
            bpTreeStore.savePage(node);
        } else {
            // index > 0 在叶子节点找到对应关键字， 直接替换为新值
            node.setValue(index, value);
            // 更新到磁盘
            bpTreeStore.savePage(node);
        }
    }


    public Integer getNextPageIndex() {
        if (isAutoCommit) {
            return bpTreeStore.getNextPageIndex();
        } else {
            Integer next = nextPageIndex;
            nextPageIndex++;
            return next;
        }
    }


    public void putUnSaveDisk(K key, V value) {
        // 从根节点往下找，并且记录查找路径
        CursorPos<K,V> cursor = CursorPos.traverseDown(rootNode, key);
        int index = cursor.getIndex();
        Page<K, V> node = cursor.getTreeNode();
        cursor = cursor.getParent();
        // index < 0表示找不到关键字,-1表示比任何关键字要小。其他数值计算通过(-index - 1)可以知道要插入的位置
        if (index < 0) {
            // 插入到叶子节点，b树每次插入都是在叶子节点插入
            node.insertLeaf(-index - 1, key, value);
            int keyCount;
            // 判断b树节点是否要分裂，当结点数大于最大节点树。关键子和值从中间分裂为两个节点，并会把中间关键字提取到父节点
            while ((keyCount = node.getKeywordCount()) > maxNodeNum
                    || node.getCurrMaxByteLen() > Page.PAGE_SIZE) {
                // 相当于除2
                int at = keyCount >> 1;
                //获取中间关键字
                K k = node.getKey(at);
                // 从中间分裂出新节点
                Page<K, V> split = node.split(at);
                // 如果父节点是空，表示当前就是根节点, 新建新节点作为父节点，原先分裂的两个节点作为其子节点
                if (cursor == null) {
                    // 设置关键字
                    List<K> keys = new ArrayList<>();
                    keys.add(k);
                    // 设置子节点
                    List<Page<K, V>> children = new ArrayList<>(2);
                    children.add(node);
                    children.add(split);
                    int nextPageIndex = getNextPageIndex();
                    rootNode = new Page<>(this, keys, null, children, false, nextPageIndex);
                    level++;
                    // 结束
                    break;
                }

                // 当前节点赋值给c
                Page<K, V> c = node;
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


    public void commitSaveDisk() {
        long startPos = rootNode.getStartPos();
        bpTreeStore.updateRootPos(startPos);
        saveDisk(rootNode);

    }

    public void saveDisk(Page<K, V> node) {
        if (node.isLeaf()) {
            bpTreeStore.savePage(node);
        } else {
            bpTreeStore.savePage(node);
            List<Page<K, V>> childNodeList = node.getChildNodeList();
            if(childNodeList == null) {
                System.out.println("1");
            }
            for (Page<K, V> childNode : childNodeList) {
                saveDisk(childNode);
            }
        }
    }





    public Page<K, V> getRootNode() {
        return rootNode;
    }

    public int getLevel() {
        return level;
    }

    public DataType<K> getKeyType() {
        return keyType;
    }

    public void setKeyType(DataType<K> keyType) {
        this.keyType = keyType;
    }

    public DataType<V> getValueType() {
        return valueType;
    }

    public void setValueType(DataType<V> valueType) {
        this.valueType = valueType;
    }


    public BpTreeStore getBpTreeStore() {
        return bpTreeStore;
    }


    public void close(){
        bpTreeStore.close();
    }


    public static <K extends Comparable> BpTreeMap<K, Long> getBtreeMap(Column primaryKeyColumn, Integer databaseId, String tableName) {
        String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
        String indexPath = dirPath + File.separator + tableName + ".idx";
        FileUtil.createFileIfNotExists(indexPath);
        BpTreeStore bpTreeStore = null;
        try {
            bpTreeStore = new BpTreeStore(indexPath);
            if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
                BpTreeMap<Integer, Long> bTreeMap = new BpTreeMap<>(1024, new IntColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                return (BpTreeMap<K, Long>) bTreeMap;
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
                BpTreeMap<Long, Long> bTreeMap = new BpTreeMap<>(1024, new LongColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                return (BpTreeMap<K, Long>) bTreeMap;
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                BpTreeMap<String, Long> bTreeMap = new BpTreeMap<>(1024, new StringColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                return (BpTreeMap<K, Long>) bTreeMap;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static <T extends Comparable> BpTreeMap<T, Long[]> getBpTreeMap(String indexPath, boolean isAutoCommit, Class<T> keyType) {
        FileUtil.createFileIfNotExists(indexPath);
        BpTreeStore bpTreeStore = null;
        try {

            bpTreeStore = new BpTreeStore(indexPath);
            if (Integer.class.equals(keyType)) {
                BpTreeMap<Integer, Long[]> bpTreeMap = new BpTreeMap<>(1024, new IntColumnType(), new LongArrayType(), bpTreeStore, isAutoCommit);
                bpTreeMap.initRootNode();
                return (BpTreeMap<T, Long[]>) bpTreeMap;
            } else if (Long.class.equals(keyType)) {
                BpTreeMap<Long, Long[]> bpTreeMap = new BpTreeMap<>(1024, new LongColumnType(), new LongArrayType(), bpTreeStore, isAutoCommit);
                bpTreeMap.initRootNode();
                return (BpTreeMap<T, Long[]>) bpTreeMap;
            } else if (String.class.equals(keyType)) {
                BpTreeMap<String, Long[]> bpTreeMap = new BpTreeMap<>(1024, new StringColumnType(), new LongArrayType(), bpTreeStore, isAutoCommit);
                bpTreeMap.initRootNode();
                return (BpTreeMap<T, Long[]>) bpTreeMap;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



}
