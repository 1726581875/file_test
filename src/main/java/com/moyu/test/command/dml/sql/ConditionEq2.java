package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionEq2 extends AbstractCondition2 {

    private String left;

    private String right;

    public ConditionEq2(String left, String right) {
        this.left = left;
        this.right = right;
    }


    @Override
    public boolean getResult(RowEntity row) {
        return false;
    }

    public String getLeft() {
        return left;
    }

    public void setLeft(String left) {
        this.left = left;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }
}
