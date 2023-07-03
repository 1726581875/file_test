package com.moyu.test.command.dml.sql;

import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

/**
 * @author xiaomingzhang
 * @date 2023/6/21
 */
public class ConditionRange extends AbstractCondition {

    private Column column;

    private String lowerLimit;

    private String upperLimit;

    /**
     * 可能是以下类型
     * < 、<= 、>、 >= 、BETWEEN
     */
    private String operator;


    /**
     * 是否等于子查询
     * 例如：
     * SELECT * FROM xmz_o_1 WHERE id = (SELECT MAX(id) FROM xmz_o_1)
     */
    private boolean hasSubQuery;
    /**
     * 下限子查询对象
     */
    private Query lowerSubQuery;
    /**
     * 上限子查询对象
     */
    private Query upperSubQuery;

    private Cursor lowerCursor;

    private Cursor upperCursor;
    /**
     * 子查询最终值
     */
    private Comparable subQueryLowerValue;

    private Comparable subQueryUpperValue;


    public ConditionRange(Column column, String lowerLimit, String upperLimit, String operator) {
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
                Comparable upperLimitValue =  getUpperLimitValue();
                int r1 = columnValue.compareTo(upperLimitValue);
                return OperatorConstant.LESS_THAN.equals(operator) ? r1 < 0 : r1 <= 0;
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                Comparable lowerLimitValue =  getLowerLimitValue();
                int r2 = columnValue.compareTo(lowerLimitValue);
                return OperatorConstant.GREATER_THAN.equals(operator) ? r2 > 0 : r2 >= 0;
            case OperatorConstant.BETWEEN:
                Comparable lower =  getLowerLimitValue();
                Comparable upper =  getUpperLimitValue();
                int r3 = columnValue.compareTo(upper);
                int r4 = columnValue.compareTo(lower);
                // 小于等于上限，大于等于下限
                return r3 <= 0 && r4 >= 0;
            default:
                throw new SqlIllegalException("sql语法有误");
        }
    }


    public Comparable getLowerLimitValue() {
        Comparable lowerLimitValue = null;
        if (!hasSubQuery) {
            lowerLimitValue = (Comparable) TypeConvertUtil.convertValueType(lowerLimit, column.getColumnType());
        } else {
            if (lowerCursor == null) {
                lowerCursor = lowerSubQuery.getQueryResultCursor();
                RowEntity rightRow = lowerCursor.next();
                subQueryLowerValue = rightRow == null ? null : (Comparable) rightRow.getColumns()[0].getValue();
                if (lowerCursor.next() != null) {
                    throw new SqlExecutionException("条件判断异常，子查询有多个值");
                }
                if (subQueryLowerValue != null) {
                    subQueryLowerValue = (Comparable) TypeConvertUtil.convertValueType(String.valueOf(subQueryLowerValue), column.getColumnType());
                } else {
                    throw new SqlExecutionException("查询异常，子查询为空");
                }
            }
            lowerLimitValue = subQueryLowerValue;
        }
        return lowerLimitValue;
    }


    public Comparable getUpperLimitValue() {
        Comparable upperLimitValue = null;
        if (!hasSubQuery) {
            upperLimitValue = (Comparable) TypeConvertUtil.convertValueType(upperLimit, column.getColumnType());
        } else {
            if (upperCursor == null) {
                upperCursor = upperSubQuery.getQueryResultCursor();
                RowEntity rightRow = upperCursor.next();
                subQueryUpperValue = rightRow == null ? null : (Comparable) rightRow.getColumns()[0].getValue();
                if (upperCursor.next() != null) {
                    throw new SqlExecutionException("条件判断异常，子查询有多个值");
                }
                if (subQueryUpperValue != null) {
                    subQueryUpperValue = (Comparable) TypeConvertUtil.convertValueType(String.valueOf(subQueryUpperValue), column.getColumnType());
                } else {
                    throw new SqlExecutionException("查询异常，子查询为空");
                }
            }
            upperLimitValue = subQueryUpperValue;
        }
        return upperLimitValue;
    }


    @Override
    public void close() {
        if (lowerSubQuery != null) {
            lowerSubQuery.closeQuery();
        }
        if (upperSubQuery != null) {
            upperSubQuery.closeQuery();
        }
        if (lowerCursor != null) {
            lowerCursor.close();
        }
        if (upperCursor != null) {
            upperCursor.close();
        }
    }

    public Column getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getLowerLimit() {
        return lowerLimit;
    }

    public String getUpperLimit() {
        return upperLimit;
    }

    public void setHasSubQuery(boolean hasSubQuery) {
        this.hasSubQuery = hasSubQuery;
    }

    public void setLowerSubQuery(Query lowerSubQuery) {
        this.lowerSubQuery = lowerSubQuery;
    }

    public void setUpperSubQuery(Query upperSubQuery) {
        this.upperSubQuery = upperSubQuery;
    }

    public void setSubQueryLowerValue(Comparable subQueryLowerValue) {
        this.subQueryLowerValue = subQueryLowerValue;
    }

    public void setSubQueryUpperValue(Comparable subQueryUpperValue) {
        this.subQueryUpperValue = subQueryUpperValue;
    }
}
