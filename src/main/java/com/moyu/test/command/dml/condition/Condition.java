package com.moyu.test.command.dml.condition;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class Condition {

    private String tableAlias;

    private String key;

    /**
     * @see com.moyu.test.constant.OperatorConstant
     */
    private String operator;

    private List<String> value;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<String> getValue() {
        return value;
    }

    public void setValue(List<String> value) {
        this.value = value;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }
}
