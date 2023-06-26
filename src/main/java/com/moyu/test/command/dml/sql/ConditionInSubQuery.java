package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionInSubQuery implements Condition2 {
    @Override
    public boolean getResult(RowEntity row) {
        return false;
    }

    @Override
    public void close() {

    }
}
