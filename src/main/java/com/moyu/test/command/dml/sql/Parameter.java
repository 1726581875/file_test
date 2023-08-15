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
        if (o == null) {
            return false;
        }

        if (this == o) {
            return true;
        }
        if (o instanceof Parameter) {
            Parameter parameter = (Parameter) o;
            if (value != null && value.equals(parameter.getValue())) {
                return true;
            }
        }
        return false;
    }

}
