package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.Page;
import com.moyu.test.store.data2.type.RowValue;
import com.moyu.test.store.metadata.obj.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/2
 */
public class BtreeCursor extends AbstractCursor {

    private Column[] columns;

    private BTreeMap bTreeMap;

    private Page currPage;

    private int nextRowIndex;

    public BtreeCursor(Column[] columns, BTreeMap bTreeMap) {
        this.columns = columns;
        this.bTreeMap = bTreeMap;
        this.currPage = bTreeMap.getFirstLeafPage();
        nextRowIndex = 0;
    }

    @Override
    void closeCursor() {
        bTreeMap.close();
    }

    @Override
    public RowEntity next() {
        if (closed) {
            throw new DbException("查询游标已关闭");
        }
        if(currPage == null) {
            return null;
        }

        if (nextRowIndex < currPage.getValueList().size()) {
            return getNextRow();
        }

        while (true) {
            Long rightPos = currPage.getRightPos();
            if(rightPos == null) {
                return null;
            }
            currPage = bTreeMap.getPageByPos(rightPos);
            if ((nextRowIndex = 0) < currPage.getValueList().size()) {
                return getNextRow();
            }
        }
    }


    private RowEntity getNextRow() {
        List<RowValue> valueList = currPage.getValueList();
        RowValue rowValue = valueList.get(nextRowIndex++);
        return rowValue.getRowEntity(columns);
    }

    @Override
    public void reset() {
        this.currPage = bTreeMap.getFirstLeafPage();
        nextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }
}
