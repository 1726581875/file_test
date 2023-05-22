package com.moyu.test.command.dml.function;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class CountFunction extends StatFunction {

    public CountFunction(String columnName) {
        super(columnName, 0L);
    }

    @Override
    public void stat(Column[] columns) {
        if ("*".equals(columnName)) {
            value++;
            return;
        }
        for (Column c : columns) {
            if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                value++;
                break;
            }
        }
    }
}
