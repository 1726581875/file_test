package com.moyu.test.command.dml.sql;

/**
 * @author xiaomingzhang
 * @date 2023/7/16
 */
public class Parameter {

    private Object value;

    private int index;

    public Parameter(int index, Object value) {
        this.value = value;
        this.index = index;
    }


    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
