package com.moyu.test.command.dml.expression;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/19
 */
public class ConditionBetween extends Expression {

    private Expression left;

    private Expression rightLow;

    private Expression rightUp;


    public ConditionBetween(Expression left, Expression rightLow, Expression rightUp) {
        this.left = left;
        this.rightLow = rightLow;
        this.rightUp = rightUp;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        Object leftValue = left.getValue(rowEntity);
        if (leftValue == null) {
            return false;
        }
        Comparable lower = (Comparable) rightLow.getValue(rowEntity);
        Comparable upper =  (Comparable) rightUp.getValue(rowEntity);
        int r1 = ((Comparable)leftValue).compareTo(lower);
        int r2 = ((Comparable)leftValue).compareTo(upper);
        // 小于等于上限，大于等于下限
        return r1 >= 0 && r2 <= 0;
    }

    @Override
    public Expression optimize() {
        return this;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }
}
