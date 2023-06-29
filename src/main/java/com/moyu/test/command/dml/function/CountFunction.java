package com.moyu.test.command.dml.function;

import com.moyu.test.store.metadata.obj.Column;

import java.util.HashSet;
import java.util.Set;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class CountFunction extends StatFunction {

    private boolean isDistinct;

    private Set<Object> distinctValueSet = new HashSet<>();

    public CountFunction(String columnName) {
        this(columnName, false);
    }

    public CountFunction(String columnName, boolean isDistinct) {
        super(columnName, 0L);
        this.isDistinct = isDistinct;
    }

    @Override
    public void stat(Column[] columns) {
        if ("*".equals(columnName)) {
            value++;
            return;
        }

        if (isDistinct) {
            for (Column c : columns) {
                if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                    if (!distinctValueSet.contains(c.getValue())) {
                        distinctValueSet.add(c.getValue());
                        value++;
                        break;
                    }

                }
            }
        } else {
            for (Column c : columns) {
                if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                    value++;
                    break;
                }
            }
        }
    }
}
