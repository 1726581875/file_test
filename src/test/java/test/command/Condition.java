package test.command;

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

    /**
     * 作为连接条件时候，右边表的别名
     */
    private String rightTableAlias;


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

    public String getRightTableAlias() {
        return rightTableAlias;
    }

    public void setRightTableAlias(String rightTableAlias) {
        this.rightTableAlias = rightTableAlias;
    }
}
