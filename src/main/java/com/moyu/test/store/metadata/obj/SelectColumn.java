package com.moyu.test.store.metadata.obj;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class SelectColumn {

    /**
     * 列字段
     */
    private Column column;
    /**
     * 字段名
     */
    private String selectColumnName;
    /**
     * 函数名
     * 如count、sum、max、min等等
     */
    private String functionName;
    /**
     * 函数参数
     */
    private String[] args;


    public SelectColumn(Column column, String selectColumnName, String functionName, String[] args) {
        this.column = column;
        this.selectColumnName = selectColumnName;
        this.functionName = functionName;
        this.args = args;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public String getSelectColumnName() {
        return selectColumnName;
    }

    public void setSelectColumnName(String selectColumnName) {
        this.selectColumnName = selectColumnName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }
}
