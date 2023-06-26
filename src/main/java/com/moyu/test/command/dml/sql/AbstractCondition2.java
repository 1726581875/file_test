package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public abstract class AbstractCondition2 implements Condition2 {


    @Override
    public void close() {

    }

    protected Column getColumnData(Column column, RowEntity row) {
        return row.getColumn(column.getColumnName(), column.getTableAlias());
    }

}
