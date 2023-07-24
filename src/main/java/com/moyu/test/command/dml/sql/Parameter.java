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

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        Parameter parameter = (Parameter) o;
        if(this == parameter) {
            return true;
        }
        if(value != null && value.equals(parameter.getValue())) {
            return true;
        }
        return false;
    }

}
