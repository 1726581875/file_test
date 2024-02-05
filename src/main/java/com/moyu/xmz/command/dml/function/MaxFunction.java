package com.moyu.xmz.command.dml.function;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.store.common.dto.Column;

import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class MaxFunction extends StatFunction {

    public MaxFunction(String columnName) {
        super(columnName, null);
    }

    @Override
    public void stat(Column[] columns) {
        for (Column c : columns) {
            if (columnName.equals(c.getColumnName()) && c.getValue() != null) {
                byte columnType = c.getColumnType();
                switch (columnType) {
                    case DbTypeConstant.INT_4:
                        Integer v1 = (Integer) c.getValue();
                        value = Long.valueOf(value == null ? v1 : Math.max(value, v1));
                        break;
                    case DbTypeConstant.INT_8:
                        Long v2 = (Long) c.getValue();
                        value = value == null ? v2 : Math.max(value, v2);
                        break;
                    case DbTypeConstant.TIMESTAMP:
                        Date v3 = (Date) c.getValue();
                        value = value == null ? v3.getTime() : Math.max(value, v3.getTime());
                        break;
                    default:
                        throw new SqlExecutionException("max函数计算错误，类型不正确,type:" + columnType);
                }
                break;
            }
        }
    }
}
