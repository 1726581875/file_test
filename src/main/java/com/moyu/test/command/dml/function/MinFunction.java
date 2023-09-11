package com.moyu.test.command.dml.function;

import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.metadata.obj.Column;

import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class MinFunction extends StatFunction {

    public MinFunction(String columnName) {
        super(columnName,null);
    }

    @Override
    public void stat(Column[] columns) {
        for (Column c : columns) {
            if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                byte columnType = c.getColumnType();
                switch (columnType) {
                    case ColumnTypeConstant.INT_4:
                        Integer v1 = (Integer) c.getValue();
                        value = Long.valueOf(value == null ? v1 : Math.min(value, v1));
                        break;
                    case ColumnTypeConstant.INT_8:
                        Long v2 = (Long) c.getValue();
                        value = value == null ? v2 : Math.min(value, v2);
                        break;
                    case ColumnTypeConstant.TIMESTAMP:
                        Date v3 = (Date) c.getValue();
                        value = value == null ? v3.getTime() : Math.min(value, v3.getTime());
                        break;
                    default:
                        throw new SqlExecutionException("min函数计算错误，类型不正确,type:" + columnType);
                }
                break;
            }
        }
    }
}
