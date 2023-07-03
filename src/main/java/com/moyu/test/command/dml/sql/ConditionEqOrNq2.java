package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionEqOrNq2 extends AbstractCondition {

    private String left;

    private String right;

    /**
     * true等于
     * false 不等于
     */
    private boolean isEq;

    public ConditionEqOrNq2(String left, String right, boolean isEq) {
        this.left = left;
        this.right = right;
        this.isEq = isEq;
    }


    @Override
    public boolean getResult(RowEntity row) {
        if (isEq) {
            return left.equals(right);
        } else {
            return !left.equals(right);
        }
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
