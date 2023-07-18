package com.moyu.test.command.dml.expression;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class Comparison extends Condition2 {

    private String operatorType;

    private Expression left;

    private Expression right;

    public Comparison(String operatorType, Expression left, Expression right) {
        this.operatorType = operatorType;
        this.left = left;
        this.right = right;
    }
}
