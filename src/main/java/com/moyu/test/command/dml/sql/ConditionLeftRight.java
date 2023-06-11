package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionLeftRight extends AbstractCondition2{

    private Column left;

    private Column right;

    public ConditionLeftRight(Column left, Column right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean getResult(RowEntity row) {
        return false;
    }

    public Column getLeft() {
        return left;
    }

    public void setLeft(Column left) {
        this.left = left;
    }

    public Column getRight() {
        return right;
    }

    public void setRight(Column right) {
        this.right = right;
    }
}
