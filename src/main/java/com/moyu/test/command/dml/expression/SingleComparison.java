package com.moyu.test.command.dml.expression;

import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class SingleComparison extends Condition2 {

    private String operator;

    private Expression left;

    private Expression right;

    public SingleComparison(String operator, Expression left, Expression right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public Boolean getValue(RowEntity rowEntity) {
        Object leftValue = left.getValue(rowEntity);
        if (leftValue == null) {
            return false;
        }

        Object rightValue = right.getValue(rowEntity);
        switch (operator) {
            case OperatorConstant.EQUAL:
                return leftValue.equals(rightValue);
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
                return !leftValue.equals(rightValue);
            case OperatorConstant.LIKE:
                return likeMatch((String) leftValue, (String) rightValue);
            case OperatorConstant.NOT_LIKE:
                return !likeMatch((String) leftValue, (String) rightValue);
            case OperatorConstant.IS_NULL:
                return leftValue == null;
            case OperatorConstant.IS_NOT_NULL:
                return leftValue != null;
            case OperatorConstant.LESS_THAN:
                return rightValue != null && ((Comparable) leftValue).compareTo((Comparable) rightValue) < 0;
            case OperatorConstant.LESS_THAN_OR_EQUAL:
                return rightValue != null && ((Comparable) leftValue).compareTo((Comparable) rightValue) <= 0;
            case OperatorConstant.GREATER_THAN:
                return rightValue != null && ((Comparable) leftValue).compareTo((Comparable) rightValue) > 0;
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                return rightValue != null && ((Comparable) leftValue).compareTo((Comparable) rightValue) >= 0;
            default:
                throw new SqlIllegalException("sql语法有误,不支持" + operator);
        }
    }


    private boolean likeMatch(String input, String sqlPattern){
        Pattern regex = Pattern.compile(sqlPattern.replace("%", ".*"));
        Matcher matcher = regex.matcher(input);
        return matcher.matches();
    }


    @Override
    public Value getValue(LocalSession session) {
        return null;
    }

    @Override
    public Expression optimize() {
        return null;
    }
}
