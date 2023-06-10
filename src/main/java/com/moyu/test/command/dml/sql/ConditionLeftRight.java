package com.moyu.test.command.dml.sql;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionLeftRight {

    private Column left;

    private Column right;

    public ConditionLeftRight(Column left, Column right) {
        this.left = left;
        this.right = right;
    }

}
