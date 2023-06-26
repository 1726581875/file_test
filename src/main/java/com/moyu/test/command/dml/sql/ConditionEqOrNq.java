package com.moyu.test.command.dml.sql;

import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.TypeConvertUtil;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 * 表示条件等于或者不等于
 * 如: a = 1 、a != 1
 */
public class ConditionEqOrNq extends AbstractCondition2 {

    private Column column;

    private String value;
    /**
     * true等于
     * false 不等于
     */
    private boolean isEq;

    /**
     * 是否等于子查询
     * 例如：
     * SELECT * FROM xmz_o_1 WHERE id = (SELECT MAX(id) FROM xmz_o_1)
     */
    private boolean eqSubQuery;
    /**
     * 子查询对象
     */
    private Query subQuery;
    /**
     * 子查询游标
     */
    private Cursor subQueryResultCursor;
    /**
     * 子查询最终值
     */
    private Object subQueryValue;


    public ConditionEqOrNq(Column column, String value, boolean isEq) {
        this.column = column;
        this.value = value;
        this.isEq = isEq;
    }

    @Override
    public boolean getResult(RowEntity row) {
        //获取左边行数据值
        Object columnValue = getColumnData(column, row).getValue();
        if (columnValue == null) {
            return false;
        }

        // 获取右边值
        Object rightValue = null;
        // 如果右边为子查询，则要先运行子查询拿到子查询结果，
        // 例如 id = (SELECT MAX(id) FROM xmz_o_1)
        if (eqSubQuery) {
            if (subQueryResultCursor == null) {
                subQueryResultCursor = subQuery.getQueryResultCursor();
                RowEntity rightRow = subQueryResultCursor.next();
                subQueryValue = rightRow == null ? null : rightRow.getColumns()[0].getValue();
                if (subQueryResultCursor.next() != null) {
                    throw new SqlExecutionException("等于条件判断异常，子查询有多个值");
                }
                if(subQueryValue != null) {
                    subQueryValue = TypeConvertUtil.convertValueType(String.valueOf(subQueryValue), column.getColumnType());
                }
            }
            rightValue = subQueryValue;
        } else {
            // 条件右边边为简单值，例如 id = 100
            rightValue = TypeConvertUtil.convertValueType(value, column.getColumnType());
        }

        if (isEq) {
            return columnValue.equals(rightValue);
        } else {
            return !columnValue.equals(rightValue);
        }
    }




    public Column getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    public boolean isEq() {
        return isEq;
    }

    public void setEqSubQuery(boolean eqSubQuery) {
        this.eqSubQuery = eqSubQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }

    @Override
    public void close() {
        if (subQuery != null) {
            subQuery.closeQuery();
        }
        if(subQueryResultCursor != null) {
            subQueryResultCursor.close();
        }
    }
}
