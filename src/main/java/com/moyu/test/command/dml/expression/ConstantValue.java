package com.moyu.test.command.dml.expression;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ConstantValue extends Expression {

    private Object value;

    public ConstantValue(Object value) {
        this.value = value;
    }
}
