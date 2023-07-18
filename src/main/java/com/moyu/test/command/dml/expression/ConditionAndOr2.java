package com.moyu.test.command.dml.expression;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ConditionAndOr2 extends Condition2 {

    private String type;

    private Expression left;

    private Expression right;


    public ConditionAndOr2(String type, Expression left, Expression right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }
}
