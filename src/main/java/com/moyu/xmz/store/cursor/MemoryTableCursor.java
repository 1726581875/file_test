package com.moyu.xmz.store.cursor;
import com.moyu.xmz.store.common.dto.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/8
 */
public class MemoryTableCursor extends AbstractCursor {

    private int nextIndex;

    private Column[] columns;

    private List<RowEntity> rows;


    public MemoryTableCursor(List<RowEntity> rows, Column[] columns) {
        this.rows = rows;
        this.columns = columns;
        this.nextIndex = 0;
    }

    @Override
    public RowEntity next() {

        if(rows == null || rows.size() == 0) {
            return null;
        }

        if(nextIndex > rows.size() - 1) {
            return null;
        }
        return rows.get(nextIndex++);
    }

    @Override
    public void reset() {
        nextIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        rows.clear();
    }

}
