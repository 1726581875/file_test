package com.moyu.test.store.data.cursor;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/8
 */
public class MemoryTemTableCursor implements Cursor {

    private int nextIndex;

    private List<RowEntity> rows;


    public MemoryTemTableCursor(List<RowEntity> rows) {
        this.rows = rows;
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

}
