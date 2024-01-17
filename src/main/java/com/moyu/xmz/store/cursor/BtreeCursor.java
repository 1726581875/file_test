package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.tree.Page;
import com.moyu.xmz.store.type.value.RowValue;
import com.moyu.xmz.store.common.dto.Column;

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

    public BTreeMap getBTreeMap() {
        return bTreeMap;
    }
}
