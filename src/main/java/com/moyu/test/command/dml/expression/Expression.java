package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public abstract class Expression {

    public abstract Object getValue(RowEntity rowEntity);

    public static boolean isMatch(RowEntity row, Expression condition) {
        if (condition == null || (boolean) condition.getValue(row)) {
            return true;
        }
        return false;
    }

    public abstract Expression optimize();


    public void setSelectIndexes(Query query) {

    }


}
