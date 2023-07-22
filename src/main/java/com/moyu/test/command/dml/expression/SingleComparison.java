package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class SingleComparison extends AbstractCondition {

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
    public Expression optimize() {
        switch (operator) {
            case OperatorConstant.EQUAL:
                if(left instanceof ConstantValue && right instanceof ConstantValue) {
                    if(left.getValue(null).equals(right.getValue(null))) {
                        return new ConstantValue(true);
                    } else {
                        return new ConstantValue(false);
                    }
                }
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
            case OperatorConstant.LIKE:
            case OperatorConstant.NOT_LIKE:
            case OperatorConstant.IS_NULL:
            case OperatorConstant.IS_NOT_NULL:
            case OperatorConstant.LESS_THAN:
            case OperatorConstant.LESS_THAN_OR_EQUAL:
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                break;
            default:
                throw new SqlIllegalException("sql语法有误,不支持" + operator);
        }

        return this;
    }

    @Override
    public void setSelectIndexes(Query query) {
        if(left instanceof ColumnExpression) {
            ColumnExpression leftColumn = (ColumnExpression) left;

            Object rightValue = right.getValue(new RowEntity(null));

            switch (operator) {
                case OperatorConstant.EQUAL:
                    SelectIndex selectIndex = getSelectIndex(leftColumn.getColumn(), query.getMainTable().getIndexMap(), rightValue);
                    if(selectIndex != null) {
                        query.getMainTable().getIndexList().add(selectIndex);
                    }
                    break;
            }
        }
    }

    private SelectIndex getSelectIndex(Column column,
                                       Map<String, IndexMetadata> indexMap,
                                       Object value) {
        IndexMetadata indexMetadata = indexMap != null ? indexMap.get(column.getColumnName()) : null;
        if(indexMetadata != null) {
            column.setValue(value);
            SelectIndex selectPlan = new SelectIndex();
            selectPlan.setTableName(column.getColumnName());
            selectPlan.setUseIndex(true);
            selectPlan.setIndexType(indexMetadata.getIndexType());
            selectPlan.setIndexColumn(column);
            selectPlan.setTableId(indexMetadata.getTableId());
            selectPlan.setIndexName(indexMetadata.getIndexName());
            return selectPlan;
        }
        return null;
    }


}
