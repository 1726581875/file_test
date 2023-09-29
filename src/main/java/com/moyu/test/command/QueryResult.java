package com.moyu.test.command;

import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class QueryResult {

    private SelectColumn[] selectColumns;

    private List<Object[]> resultRows = new ArrayList<>();

    /**
     * 描述
     */
    private String desc;


    public void addAll(List<Column[]> columnValueList) {
        for (Column[] columns : columnValueList) {
            addRow(columns);
        }
    }

    public void addRow(Column[] columnValues) {
        Object[] values = new Object[columnValues.length];
        for (int i = 0; i < columnValues.length; i++) {
            values[i] = columnValues[i].getValue();
        }
        addRow(values);
    }

    public void addRow(Object[] resultRow) {
        if (resultRows == null) {
            resultRows = new ArrayList<>();
        }
        resultRows.add(resultRow);
    }

    public List<Object[]> getResultRows() {
        return resultRows;
    }

    public void setResultRows(List<Object[]> resultRows) {
        this.resultRows = resultRows;
    }

    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "selectColumns=" + Arrays.toString(selectColumns) +
                ", resultRows=" + resultRows +
                '}';
    }
}
