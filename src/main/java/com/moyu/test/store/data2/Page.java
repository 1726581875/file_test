package com.moyu.test.store.data2;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.SerializableByte;
import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public abstract class Page<K, V> implements SerializableByte {
    /**
     * 每页大小，固定4KB
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * 页的固定属性占用字节长度
     * usedByteLen(4 byte) + startPos(8 byte) + pageIndex(4 byte)
     * + keywordCount(4 byte) + pageType(1 byte) + rightPos(8 byte) = 29 byte
     */
    private static final int pageBasicSize = 29;

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
     * 页类型：1叶子节点、0非叶子节点
     */
    private byte pageType;
    /**
     * 叶子节点指向右边叶子节点的位置
     */
    protected Long rightPos;

    /**
     * 当前可能占用的最大字节长度，不是真正使用字节长度
     * 用于判断节点是否需要分裂
     */
    protected int currMaxByteLen;
    /**
     * 关键字数量
     */
    protected int keywordCount;
    /**
     * 节点关键词列表
     */
    protected List<K> keywordList;

    protected BTreeMap<K, V> map;


    public Page(BTreeMap<K, V> map, List<K> keywordList, boolean isLeaf, int pageIndex) {
        this.map = map;
        this.keywordList = keywordList;
        this.keywordCount = this.keywordList.size();
        this.pageType = (byte) (isLeaf ? 1 : 0);
        // 所有页位置都往后移动一个页大小字节，因为最前面8字节用于标记根节点位置
        this.startPos = (pageIndex * PAGE_SIZE) + BTreeStore.PAGE_START_POS;
        this.pageIndex = pageIndex;
    }



    public static <K,V> Page<K,V> createNonLeaf(BTreeMap<K,V> map, List keywordList, List<Page<K,V>> childNodeList, List<Long> childPosList, int pageIndex) {
         return new NonLeaf(map, keywordList, childNodeList, childPosList, pageIndex);
    }



    public static <K,V> Page<K,V> createLeaf(BTreeMap<K,V> map, List keywordList, List<V> valueList, int pageIndex) {
        return new Leaf<>(map, keywordList, valueList, pageIndex);
    }


    public static <K,V> Page<K,V> readPageByByteBuffer(ByteBuffer byteBuffer, BTreeMap<K,V> treeMap) {
        int usedByteLen = byteBuffer.getInt();
        long startPos = byteBuffer.getLong();
        int pageIndex = byteBuffer.getInt();
        int keywordCount = byteBuffer.getInt();
        byte pageType = byteBuffer.get();
        // 指向右边指针，叶子节点才有
        Long rightPos = null;
        long rPos = byteBuffer.getLong();
        if(rPos < 0) {
            rightPos = null;
        } else {
            rightPos = rPos;
        }
        // 关键字
        List<K> keywordList = new ArrayList<>(keywordCount);
        for (int i = 0; i < keywordCount; i++) {
            K k = treeMap.getKeyType().read(byteBuffer);
            keywordList.add(k);
        }
        // 0为非叶子节点，1为叶子节点
        List<Long> childPosList = null;
        if (pageType == (byte) 0) {

            if (keywordCount > 0) {
                childPosList = new ArrayList<>(keywordCount + 1);
                for (int i = 0; i <= keywordCount; i++) {
                    childPosList.add(byteBuffer.getLong());
                }
            }
            NonLeaf nonLeaf = new NonLeaf(treeMap, keywordList, null, childPosList, pageIndex);
            nonLeaf.setStartPos(startPos);
            nonLeaf.setUsedByteLen(usedByteLen);
            return nonLeaf;

        } else {
            List<V> valueList = new ArrayList<>(keywordCount);
            for (int i = 0; i < keywordCount; i++) {
                V v = treeMap.getValueType().read(byteBuffer);
                valueList.add(v);
            }

            Leaf<K, V> kvLeaf = new Leaf<>(treeMap, keywordList, valueList, pageIndex);
            kvLeaf.setRightPos(rightPos);
            kvLeaf.setStartPos(startPos);
            kvLeaf.setUsedByteLen(usedByteLen);
            return kvLeaf;
        }
    }

/*    public Page(ByteBuffer byteBuffer, BTreeMap treeMap) {
        this.usedByteLen = byteBuffer.getInt();
        this.startPos = byteBuffer.getLong();
        this.pageIndex = byteBuffer.getInt();
        this.keywordCount = byteBuffer.getInt();
        this.pageType = byteBuffer.get();
        // 指向右边指针，叶子节点才有
        long rPos = byteBuffer.getLong();
        if(rPos < 0) {
            this.rightPos = null;
        } else {
            this.rightPos = rPos;
        }

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
        resetCountCrrMaxSize();
    }*/





    protected int resetCountCrrMaxSize() {
        this.currMaxByteLen = pageBasicSize;
        if (keywordList != null) {
            for (K k : keywordList) {
                this.currMaxByteLen += map.getKeyType().getMaxByteSize(k);
            }
        }
        countMaxSize();
        return this.currMaxByteLen;
    }


    protected abstract int countMaxSize();


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        // usedByteLen 实际占用字节长度,后面会复写
        writeBuffer.putInt(0);
        writeBuffer.putLong(this.startPos);
        writeBuffer.putInt(this.pageIndex);
        writeBuffer.putInt(this.keywordCount);
        writeBuffer.put(this.pageType);
        writeBuffer.putLong(this.rightPos == null ? -1L : this.rightPos);
        // 关键字
        for (int i = 0; i < this.keywordCount; i++) {
            map.getKeyType().write(writeBuffer, this.keywordList.get(i));
        }
        // 写入节点包含的值/指针
        writeValue(writeBuffer);

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



    public K getKeyword(int index) {
        return this.keywordList.get(index);
    }


    protected abstract void writeValue(WriteBuffer writeBuffer);

    public abstract Page<K, V> split(int index);


    public abstract void remove(int i);

    public abstract void insertLeaf(int index, K key, V value);


    public abstract void setChild(int index, Page<K, V> node);


    public abstract void insertNonLeaf(int index, K key, Page<K, V> node);


    public abstract void setLeafValue(int index, V value);

    public abstract List<Page<K, V>> getChildNodeList();

    public abstract List<Long> getChildPosList();

    public abstract boolean isLeaf();

    public abstract List<V> getValueList();


    /**
     * 返回-1，表示比所有关键词都要小
     * 返回其他负数，表示找不到关键字，返回值为key应当插入的位置
     * 返回正数或者0，表示找到一样的关键字，其下标为返回值
     *
     * @param key
     * @return
     */
    public int binarySearch(K key) {
        int left = 0;
        int right = this.keywordList.size() - 1;

        int mid = 0;
        while (left <= right) {
            // 求中间值
            mid = (left + right) >>> 1;
            K currKey = this.keywordList.get(mid);
            // 当前关键字等于key，返回位置
            if (map.getKeyType().compare(currKey, key) == 0) {
                return mid;
            } else if /* 当前关键字小于key,说明再右侧，缩小范围继续查找 */ (map.getKeyType().compare(currKey, key) < 0) {
                left = mid + 1;
            } else /* 当前关键字大于key,说明再左侧，缩小范围继续查找 */{
                right = mid - 1;
            }
        }

        // 如果找不到目标元素
        return -(left + 1);
    }


    private static class Leaf<K,V> extends Page <K,V> {
        /**
         * 值列表
         */
        private List<V> valueList;

        public Leaf(BTreeMap<K, V> map, List<K> keywordList, List<V> valueList, int pageIndex) {
            super(map, keywordList, false, pageIndex);
            this.valueList = valueList;
            resetCountCrrMaxSize();
        }


        @Override
        protected int countMaxSize() {
            if (valueList != null) {
                for (V v : valueList) {
                    this.currMaxByteLen += map.getValueType().getMaxByteSize(v);
                }
            }
            return this.currMaxByteLen;
        }

        @Override
        protected void writeValue(WriteBuffer writeBuffer) {
            // 写入叶子节点的值
            for (int i = 0; i < this.keywordCount; i++) {
                map.getValueType().write(writeBuffer, this.valueList.get(i));
            }
        }

        @Override
        public Page<K, V> split(int index) {
            // 分裂关键字
            List<K> rightKeywords = splitLeafKey(index);
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
            resetCountCrrMaxSize();

            // 创建右节点
            Leaf<K, V> rightPage = new Leaf<>(map, rightKeywords, rightValues,  map.getNextPageIndex());
            // 调整叶子节点指针指向
            rightPage.setRightPos(rightPos);
            this.rightPos = rightPage.getStartPos();

            return rightPage;
        }

        @Override
        public void remove(int i) {
            this.keywordList.remove(i);
            this.valueList.remove(i);
            this.keywordCount--;
        }

        @Override
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

        @Override
        public void setChild(int index, Page<K, V> node) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNonLeaf(int index, K key, Page<K, V> node) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLeafValue(int index, V value) {
            this.valueList.set(index, value);
        }

        @Override
        public List<Page<K, V>> getChildNodeList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Long> getChildPosList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLeaf() {
            return true;
        }

        @Override
        public List<V> getValueList() {
            return valueList;
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

        public void setRightPos(Long rightPos) {
            this.rightPos = rightPos;
        }
    }

    private static class NonLeaf<K,V> extends Page <K,V> {
        /**
         * 树子节点列表
         */
        private List<Page<K, V>> childNodeList;
        /**
         * 子节点对应文件位置列表
         */
        private List<Long> childPosList;

        public NonLeaf(BTreeMap<K, V> map, List<K> keywordList, List<Page<K, V>> childNodeList, List<Long> childPosList, int pageIndex) {
            super(map, keywordList, false, pageIndex);
            this.childNodeList = childNodeList;
            this.childPosList = childPosList;
            resetCountCrrMaxSize();
        }


        @Override
        protected int countMaxSize() {
            if (this.childNodeList != null && this.childNodeList.size() > 0) {
                this.currMaxByteLen += this.childNodeList.size() * 8;
            } else {
                this.currMaxByteLen += this.childPosList.size() * 8;
            }
            return this.currMaxByteLen;
        }

        @Override
        protected void writeValue(WriteBuffer writeBuffer) {

            if(this.childNodeList != null && this.childNodeList.size() > 0) {
                this.childPosList = this.childNodeList.stream().map(Page::getStartPos).collect(Collectors.toList());
            }

            // 写入非叶子节点包含的指针(子页的文件内开始位置)，子节点数量为keywordCount + 1
            for (int i = 0; i <= this.keywordCount; i++) {
                writeBuffer.putLong(this.childPosList.get(i));
            }
        }

        @Override
        public Page<K, V> split(int index) {
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
            }

            // 分裂磁盘指针
            if(this.childPosList != null) {
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
            resetCountCrrMaxSize();

            return new NonLeaf<>(map, rightKeywords, rightChildNodes, rightChildPosList, map.getNextPageIndex());
        }

        @Override
        public void remove(int i) {
            this.keywordList.remove(i);
            if (this.childPosList != null && this.childPosList.size() > 0) {
                this.childPosList.remove(i);
            }
            if (this.childNodeList != null && this.childNodeList.size() > 0) {
                this.childNodeList.remove(i);
            }
            this.keywordCount--;
        }

        @Override
        public void insertLeaf(int index, K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChild(int index, Page<K, V> node) {
            if (this.childNodeList != null) {
                this.childNodeList.set(index, node);
            } else if (this.getChildPosList() != null) {
                this.childPosList.set(index, node.getStartPos());
            } else {
                throw new DbException("设置子节点异常");
            }
        }

        @Override
        public void insertNonLeaf(int index, K key, Page<K, V> node) {
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

        @Override
        public void setLeafValue(int index, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Page<K, V>> getChildNodeList() {
            return childNodeList;
        }

        @Override
        public List<Long> getChildPosList() {
            return childPosList;
        }

        @Override
        public boolean isLeaf() {
            return false;
        }

        @Override
        public List<V> getValueList() {
            throw new UnsupportedOperationException();
        }

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


    public int getKeywordCount() {
        return keywordCount;
    }

    public int getCurrMaxByteLen() {
        return currMaxByteLen;
    }


    public void setMap(BTreeMap<K, V> map) {
        this.map = map;
    }

    public BTreeMap<K, V> getMap() {
        return map;
    }

}
