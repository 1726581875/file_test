package com.moyu.test.command.dml.sql;

import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

/**
 * @author xiaomingzhang
 * @date 2023/6/21
 */
public class ConditionRange extends AbstractCondition2 {

    private Column column;

    private String lowerLimit;

    private String upperLimit;

    /**
     * 可能是以下类型
     * < 、<= 、>、 >= 、BETWEEN
     */
    private String operator;


    public ConditionRange(Column column,String lowerLimit, String upperLimit, String operator) {
        this.column = column;
        this.upperLimit = upperLimit;
        this.lowerLimit = lowerLimit;
        this.operator = operator;
    }

    @Override
    public boolean getResult(RowEntity row) {
        Column columnData = getColumnData(column, row);
        Comparable columnValue = (Comparable) columnData.getValue();

        if(columnValue == null) {
            return false;
        }
        switch (operator) {
            case OperatorConstant.LESS_THAN:
            case OperatorConstant.LESS_THAN_OR_EQUAL:
                Comparable upperLimitValue =  (Comparable) TypeConvertUtil.convertValueType(upperLimit, column.getColumnType());
                int r1 = columnValue.compareTo(upperLimitValue);
                return OperatorConstant.LESS_THAN.equals(operator) ? r1 < 0 : r1 <= 0;
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                Comparable lowerLimitValue =  (Comparable) TypeConvertUtil.convertValueType(lowerLimit, column.getColumnType());
                int r2 = columnValue.compareTo(lowerLimitValue);
                return OperatorConstant.GREATER_THAN.equals(operator) ? r2 > 0 : r2 >= 0;
            case OperatorConstant.BETWEEN:
                // 下限
                Comparable lower =  (Comparable) TypeConvertUtil.convertValueType(lowerLimit, column.getColumnType());
                // 上限
                Comparable upper =  (Comparable) TypeConvertUtil.convertValueType(upperLimit, column.getColumnType());
                int r3 = columnValue.compareTo(upper);
                int r4 = columnValue.compareTo(lower);
                // 小于等于上限，大于等于下限
                return r3 <= 0 && r4 >= 0;
            default:
                throw new SqlIllegalException("sql语法有误");
        }
    }
}
