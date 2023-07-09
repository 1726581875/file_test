package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.type.RowValue;
import com.moyu.test.store.data2.type.Value;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/9
 */
public class BTreeIndexCursor extends AbstractCursor {

    private Column[] columns;

    private BTreeMap clusteredIndexMap;

    private Value[]  keyArray;

    private int nextIndex;

    public BTreeIndexCursor(Column[] columns, BTreeMap clusteredIndexMap, Value[] keyArray) {
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

        Object keyValue = keyArray[nextIndex++].getObjValue();
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
