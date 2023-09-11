package com.moyu.test.command.dml.function;

import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class SumFunction extends StatFunction{

    public SumFunction(String columnName) {
        super(columnName,0L);
    }

    @Override
    public void stat(Column[] columns) {
        for (Column c : columns) {
            if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                byte columnType = c.getColumnType();
                switch (columnType) {
                    case ColumnTypeConstant.INT_4:
                        Integer v1 = (Integer) c.getValue();
                        value = Long.valueOf(value == null ? v1 : value + v1);
                        break;
                    case ColumnTypeConstant.INT_8:
                        Long v2 = (Long) c.getValue();
                        value = value == null ? v2 : value + v2;
                        break;
                    case ColumnTypeConstant.TIMESTAMP:
                        throw new SqlExecutionException("sum函数计算错误，不支持日期类型");
                    default:
                        throw new SqlExecutionException("sum函数计算错误，类型不正确,type:" + columnType);
                }
                break;
            }
        }
    }
}
