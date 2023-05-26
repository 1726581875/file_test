package com.moyu.test.store.data.tree;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.SerializableByte;
import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class Page<K extends Comparable, V> implements SerializableByte {
    /**
     * 每页大小，固定4KB
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * 已使用字节长度,包括本身
     */
    private int usedByteLen;
    /**
     * 页开始位置
     */
    private long startPos;
    /**
     * 页下标
     */
    private int pageIndex;
    /**
     * 关键字数量
     */
    private int keywordCount;
    /**
     * 页类型：1叶子节点、0非叶子节点
     */
    private byte pageType;

    /**
     * 当前可能占用的最大字节长度，不是真正使用字节长度
     * 用于判断节点是否需要分裂
     */
    private int currMaxByteLen;


    private BTreeMap<K, V> map;
    /**
     * 节点关键词列表
     */
    private List<K> keywordList;

    /**
     * 树子节点列表
     */
    private List<Page<K, V>> childNodeList;
    /**
     * 子节点对应文件位置列表
     */
    private List<Long> childPosList;

    /**
     * 是否是叶子节点
     */
    private boolean isLeaf;
    /**
     * 值列表
     */
    private List<V> valueList;


    public Page(BTreeMap<K, V> map,
                List<K> keywordList,
                List<V> valueList,
                List<Page<K, V>> childNodeList,
                boolean isLeaf,
                int pageIndex) {
        this(map, keywordList, valueList, childNodeList, null, isLeaf, pageIndex);
    }


    public Page(BTreeMap<K, V> map,
                List<K> keywordList,
                List<V> valueList,
                List<Page<K, V>> childNodeList,
                List<Long> childPosList,
                boolean isLeaf,
                int pageIndex) {
        this.map = map;
        this.keywordList = keywordList;
        this.valueList = valueList;
        this.childNodeList = childNodeList;
        this.childPosList = childPosList;
        this.isLeaf = isLeaf;

        // 计算当前最大字节长度
        int basicLen = 4 + 1 + 8 + 4 + 4;
        this.currMaxByteLen = basicLen;
        if (keywordList != null) {
            for (K k : keywordList) {
                this.currMaxByteLen += map.getKeyType().getMaxByteSize(k);
            }
        }
        if (valueList != null) {
            for (V v : valueList) {
                this.currMaxByteLen += map.getValueType().getMaxByteSize(v);
            }
        }
        if (childNodeList != null) {
            this.currMaxByteLen += childNodeList.size() * 8;
        }

        this.keywordCount = this.keywordList.size();
        this.pageType = (byte) (isLeaf ? 1 : 0);
        // 所有页位置都往后移动8字节，因为最前面8字节用于标记根节点位置
        this.startPos = (pageIndex * PAGE_SIZE) + JavaTypeConstant.LONG_LENGTH;
        this.pageIndex = pageIndex;
        this.usedByteLen = this.currMaxByteLen;
    }


    public Page(ByteBuffer byteBuffer, BTreeMap treeMap) {
        this.usedByteLen = byteBuffer.getInt();
        this.startPos = byteBuffer.getLong();
        this.pageIndex = byteBuffer.getInt();
        this.keywordCount = byteBuffer.getInt();
        this.pageType = byteBuffer.get();

        this.map = treeMap;

        // 关键字
        this.keywordList = new ArrayList<>(this.keywordCount);
        for (int i = 0; i < this.keywordCount; i++) {
            K k = this.map.getKeyType().read(byteBuffer);
            this.keywordList.add(k);
        }
        // 0为非叶子节点，1为叶子节点
        if (this.pageType == (byte) 0) {
            if (this.keywordCount > 0) {
                this.childPosList = new ArrayList<>(this.keywordCount + 1);
                //DataType<Long> longType = ColumnTypeFactory.getColumnType(ColumnTypeEnum.BIGINT.getColumnType());
                for (int i = 0; i <= this.keywordCount; i++) {
                    this.childPosList.add(byteBuffer.getLong());
                }
            }
        } else {
            // 值
            this.valueList = new ArrayList<>(this.keywordCount);
            for (int i = 0; i < this.keywordCount; i++) {
                V v = this.map.getValueType().read(byteBuffer);
                this.valueList.add(v);
            }
        }
        this.isLeaf = this.pageType == 1;
        resetCrrMaxByteLen();
    }


    private void resetCrrMaxByteLen() {
        int basicLen = 4 + 1 + 8 + 4 + 4;
        this.currMaxByteLen = basicLen;
        if (keywordList != null) {
            for (K k : keywordList) {
                this.currMaxByteLen += map.getKeyType().getMaxByteSize(k);
            }
        }
        if (valueList != null) {
            for (V v : valueList) {
                this.currMaxByteLen += map.getValueType().getMaxByteSize(v);
            }
        }

        if (this.childNodeList != null || this.childPosList != null) {
            if (this.childNodeList != null) {
                this.currMaxByteLen += this.childNodeList.size() * 8;
            } else {
                this.currMaxByteLen += this.childPosList.size() * 8;
            }
        }

    }


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        // usedByteLen 实际占用字节长度,后面会复写
        writeBuffer.putInt(0);
        writeBuffer.putLong(this.startPos);
        writeBuffer.putInt(this.pageIndex);
        writeBuffer.putInt(this.keywordCount);
        writeBuffer.put(this.pageType);

        // 关键字
        for (int i = 0; i < this.keywordCount; i++) {
            map.getKeyType().write(writeBuffer, this.keywordList.get(i));
        }

        // 0为非叶子节点，1为叶子节点
        if (this.pageType == (byte) 0) {
            if (this.keywordCount > 0) {
                // 写入子节点的文件位置，子节点数量为keywordCount + 1
                //DataType longType = ColumnTypeFactory.getColumnType(ColumnTypeEnum.BIGINT.getColumnType());
                for (int i = 0; i <= this.keywordCount; i++) {
                    long startPos = this.childPosList != null ? this.childPosList.get(i) : this.childNodeList.get(i).getStartPos();
                    //longType.write(writeBuffer, startPos);
                    writeBuffer.putLong(startPos);
                }
            }
        } else {
            // 值
            for (int i = 0; i < this.keywordCount; i++) {
                map.getValueType().write(writeBuffer, this.valueList.get(i));
            }
        }
        // 复写占用字节长度
        int position = writeBuffer.position();
        writeBuffer.putInt(0, position);
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        if (buffer.limit() > PAGE_SIZE) {
            throw new RuntimeException("页大小超出限制:" + buffer.limit());
        }

        return buffer;
    }


    public static <K extends Comparable, V> V get(Page<K, V> p, K key) {
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
        if (index == this.keywordList.size()) {
            this.keywordList.add(key);
            this.valueList.add(value);
        } else {
            this.keywordList.add(index, key);
            this.valueList.add(index, value);
        }
        this.keywordCount++;
        this.currMaxByteLen += map.getKeyType().getMaxByteSize(key);
        this.currMaxByteLen += map.getValueType().getMaxByteSize(value);
    }


    public K getKey(int index) {
        return this.keywordList.get(index);
    }


    public Page<K, V> split(int index) {

        // 叶子节点,叶子节点的关键字数和值数量一样
        if (isLeaf) {
            // 分裂关键字
            List<K> rightKeywords = splitLeafKey(index);
            ;
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
            this.keywordCount = this.keywordList.size();
            resetCrrMaxByteLen();
            return createLeafNode(rightKeywords, rightValues);



            // 非叶子节点分裂
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
            this.keywordCount = this.keywordList.size();
            // 分裂孩子节点
            List<Page<K, V>> rightChildNodes = new ArrayList<>();
            List<Long> rightChildPosList = new ArrayList<>();
            // 分裂内存指针
            if(this.childNodeList != null) {
                List<Page<K, V>> leftChildNodes = new ArrayList<>(index);
                for (int i = 0; i < this.childNodeList.size(); i++) {
                    Page<K, V> node = this.childNodeList.get(i);
                    if (i < index + 1) {
                        leftChildNodes.add(node);
                    } else {
                        rightChildNodes.add(node);
                    }
                }
                this.childNodeList = leftChildNodes;
                this.keywordCount = this.keywordList.size();
            } else {
                // 分裂磁盘指针
                List<Long> leftChildNodes = new ArrayList<>(this.childPosList.size() - index);
                for (int i = 0; i < this.childPosList.size(); i++) {
                    Long pos = this.childPosList.get(i);
                    if (i < index + 1) {
                        leftChildNodes.add(pos);
                    } else {
                        rightChildPosList.add(pos);
                    }
                }
                this.childPosList = leftChildNodes;
            }
            resetCrrMaxByteLen();

            return createNonLeafNode(rightKeywords, rightChildNodes, childPosList);
        }
    }


    private Page<K, V> createLeafNode(List<K> keywordList, List<V> valueList) {
        return new Page<>(map, keywordList, valueList, null, true, map.getBpTreeStore().getNextPageIndex());
    }


    private Page<K, V> createNonLeafNode(List<K> keywordList, List<Page<K, V>> childNodeList, List<Long> childPosList) {
        return new Page<>(map, keywordList, null, childNodeList, childPosList, false, map.getBpTreeStore().getNextPageIndex());
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


    public void setChild(int index, Page<K, V> node) {
        if (this.childNodeList != null) {
            this.childNodeList.set(index, node);
        } else if (this.getChildPosList() != null) {
            this.childPosList.set(index, node.getStartPos());
        } else {
            throw new IllegalArgumentException("设置子节点异常");
        }
    }

    public void insertNode(int index, K key, Page<K, V> node) {
        // 插入关键字
        if (index >= this.keywordList.size()) {
            this.keywordList.add(key);
        } else {
            this.keywordList.add(index, key);
        }

        // 插入节点
        if (this.childNodeList != null) {
            if (index >= this.childNodeList.size()) {
                this.childNodeList.add(node);
            } else {
                this.childNodeList.add(index, node);
            }
        } else {
            if (index >= this.childPosList.size()) {
                this.childPosList.add(node.getStartPos());
            } else {
                this.childPosList.add(index, node.getStartPos());
            }
        }
        this.keywordCount++;
        this.currMaxByteLen += 8 + map.getKeyType().getMaxByteSize(key);
    }

    public void setValue(int index, V value) {
        this.valueList.set(index, value);
    }


    /**
     * 返回-1，表示比所有关键词都要小
     * 返回负数，表示找不到关键字，返回值为关键字插入位置
     * 返回正数，表示找到一样的关键字，其下标为返回值
     *
     * @param key
     * @return
     */
    public int binarySearch(K key) {
        if (this.keywordList.size() == 0 || key.compareTo(this.keywordList.get(0)) < 0) {
            return -1;
        }
        for (int i = 0; i < this.keywordList.size(); i++) {
            K currKey = this.keywordList.get(i);
            if (currKey.compareTo(key) == 0) {
                return i;
                // this.keywordList是从小到大，当找到第一个比key大的值，当前的i就是插入位置
            } else if (currKey.compareTo(key) > 0) {
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

    public List<Page<K, V>> getChildNodeList() {
        return childNodeList;
    }

    public List<Long> getChildPosList() {
        return childPosList;
    }

    public void setChildNodeList(List<Page<K, V>> childNodeList) {
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

    private void appendString(Integer parent, Page<K, V> node, StringBuilder stringBuilder) {
        if (node.isLeaf) {
            appendToString(parent, -1, node, stringBuilder);
        } else {
            appendToString(parent, 0, node, stringBuilder);
            if (childNodeList == null) {
                return;
            }
            List<Page<K, V>> childNodeList = node.getChildNodeList();
            Integer p = parent + 1;
            for (int i = 0; i < childNodeList.size(); i++) {
                Page<K, V> treeNode = childNodeList.get(i);
                appendToString(p, i, treeNode, stringBuilder);
            }
        }

    }


    private void appendToString(Integer parent, int index, Page<K, V> node, StringBuilder stringBuilder) {
        stringBuilder.append("[" + parent + "]:" + index + ">>");
        if (node.isLeaf()) {
            stringBuilder.append("[leaf] " + "keys=" + node.getKeywordList() + "    values=" + node.getValueList());
        } else {
            stringBuilder.append("[nonLeaf] " + "keys=" + node.getKeywordList());
        }
        stringBuilder.append("\n");
    }


    public int getUsedByteLen() {
        return usedByteLen;
    }

    public void setUsedByteLen(int usedByteLen) {
        this.usedByteLen = usedByteLen;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getKeywordCount() {
        return keywordCount;
    }

    public void setKeywordCount(int keywordCount) {
        this.keywordCount = keywordCount;
    }

    public byte getPageType() {
        return pageType;
    }

    public void setPageType(byte pageType) {
        this.pageType = pageType;
    }

    public int getCurrMaxByteLen() {
        return currMaxByteLen;
    }

    public void setCurrMaxByteLen(int currMaxByteLen) {
        this.currMaxByteLen = currMaxByteLen;
    }


    public void setMap(BTreeMap<K, V> map) {
        this.map = map;
    }

    public BTreeMap<K, V> getMap() {
        return map;
    }
}
