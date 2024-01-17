package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.type.value.RowValue;
import com.moyu.xmz.store.common.dto.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/9
 */
public class BTreeIndexCursor extends AbstractCursor {

    private Column[] columns;

    private BTreeMap clusteredIndexMap;

    private Object[]  keyArray;

    private int nextIndex;

    public BTreeIndexCursor(Column[] columns, BTreeMap clusteredIndexMap, Object[] keyArray) {
        this.columns = columns;
        this.clusteredIndexMap = clusteredIndexMap;
        this.keyArray = keyArray;
        this.nextIndex = 0;
    }

    @Override
    void closeCursor() {
        clusteredIndexMap.close();
    }

    @Override
    public RowEntity next() {
        if(closed) {
            throw new DbException("游标已关闭");
        }
        if (keyArray == null || nextIndex >= keyArray.length) {
            return null;
        }

        Object keyValue = keyArray[nextIndex++];
        RowValue rowValue = (RowValue)clusteredIndexMap.get(keyValue);
        if(rowValue == null) {
            throw new DbException("根据根据键查询结果应当不为空，key:" + keyValue);
        }

        RowEntity rowEntity = rowValue.getRowEntity(columns);
        return rowEntity;
    }

    @Override
    public void reset() {
        nextIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }
}
