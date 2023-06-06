package com.moyu.test.store.data.cursor;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class RowEntity {

    private Column[] columns;

    public RowEntity(Column[] columns) {
        this.columns = columns;
    }

    public Column[] getColumns() {
        return columns;
    }

}
