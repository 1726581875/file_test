package com.moyu.xmz.command.dml.function;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.store.common.dto.Column;
/**
 * @author xiaomingzhang
 * @date 2023/6/26
 */
public class AvgFunction extends StatFunction {

    private int recordNum;

    private Double totalValue;

    public AvgFunction(String columnName) {
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
                        totalValue = Double.valueOf(totalValue == null ? v1 : totalValue + v1);
                        break;
                    case DbTypeConstant.INT_8:
                        Long v2 = (Long) c.getValue();
                        totalValue = totalValue == null ? v2 : totalValue + v2;
                        break;
                    case DbTypeConstant.TIMESTAMP:
                        throw new SqlExecutionException("sum函数计算错误，不支持日期类型");
                    default:
                        throw new SqlExecutionException("sum函数计算错误，类型不正确,type:" + columnType);
                }
                recordNum++;
                break;
            }
        }
    }

    public Double getAvgValue() {
        if(totalValue == null) {
            return null;
        }
        if(recordNum == 0) {
            return null;
        }
        return (totalValue / recordNum);
    }
}
