package com.moyu.test.command;

import com.moyu.test.store.metadata.obj.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class QueryResult {

    private Column[] columns;

    private List<Object[]> resultRows;

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

    public Column[] getColumns() {
        return columns;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public List<Object[]> getResultRows() {
        return resultRows;
    }

    public void setResultRows(List<Object[]> resultRows) {
        this.resultRows = resultRows;
    }
}
