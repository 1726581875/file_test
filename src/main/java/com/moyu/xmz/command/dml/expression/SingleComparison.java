package com.moyu.xmz.command.dml.expression;

import com.moyu.xmz.command.dml.plan.SelectIndex;
import com.moyu.xmz.command.dml.sql.Query;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.common.constant.OperatorConstant;
import com.moyu.xmz.common.exception.SqlIllegalException;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.IndexMeta;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 * 常用的比较单个值表达式，入column=1、column like '%%' 、column is not null
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
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
                // 常量相等，如 1 = 1
                if(left instanceof ConstantValue && right instanceof ConstantValue) {
                    boolean result = constantLeftEqRight();
                    if(OperatorConstant.EQUAL.equals(operator)) {
                        return ConstantValue.getBooleanExpression(result);
                    } else {
                        return ConstantValue.getBooleanExpression(!result);
                    }
                }
                break;
            case OperatorConstant.LIKE:
            case OperatorConstant.NOT_LIKE:
                // 常量字符like,如 'name' like 'nam%'
                if(left instanceof ConstantValue && right instanceof ConstantValue) {
                    Object leftValue = left.getValue(null);
                    Object rightValue = right.getValue(null);
                    if(leftValue instanceof String && rightValue instanceof String){
                        boolean result = likeMatch((String) leftValue, (String) rightValue);
                        if(OperatorConstant.LIKE.equals(operator)) {
                            return ConstantValue.getBooleanExpression(result);
                        } else {
                            return ConstantValue.getBooleanExpression(!result);
                        }
                    }
                }
                break;
            case OperatorConstant.IS_NULL:
                break;
            case OperatorConstant.IS_NOT_NULL:
                break;
            case OperatorConstant.LESS_THAN:
            case OperatorConstant.LESS_THAN_OR_EQUAL:
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                // 对比两个常量大大小，如 1 < 2
                if(left instanceof ConstantValue && right instanceof ConstantValue) {
                    Object leftValue = left.getValue(null);
                    Object rightValue = right.getValue(null);
                    if(leftValue instanceof Comparable && rightValue instanceof Comparable) {
                        boolean result = getLessOrGreResult((Comparable)leftValue, (Comparable)rightValue, operator);
                        return ConstantValue.getBooleanExpression(result);
                    }
                }
                break;
            default:
                throw new SqlIllegalException("sql语法有误,不支持" + operator);
        }

        return this;
    }

    private boolean getLessOrGreResult(Comparable leftValue, Comparable rightValue, String operator) {
        switch (operator) {
            case OperatorConstant.LESS_THAN:
                return rightValue != null && (leftValue).compareTo(rightValue) < 0;
            case OperatorConstant.LESS_THAN_OR_EQUAL:
                return rightValue != null && (leftValue).compareTo(rightValue) <= 0;
            case OperatorConstant.GREATER_THAN:
                return rightValue != null && (leftValue).compareTo(rightValue) > 0;
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                return rightValue != null && (leftValue).compareTo(rightValue) >= 0;
            default:
                throw new SqlIllegalException("sql语法有误,不支持" + operator);
        }
    }


    private boolean constantLeftEqRight() {
        return left.getValue(null).equals(right.getValue(null));
    }

    @Override
    public void setSelectIndexes(Query query) {
        if(left instanceof ConditionColumnExpr && !(right instanceof ConditionColumnExpr)) {
            ConditionColumnExpr leftColumn = (ConditionColumnExpr) left;
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

    @Override
    public void getSQL(StringBuilder sqlBuilder) {
        left.getSQL(sqlBuilder);
        sqlBuilder.append(operator);
        right.getSQL(sqlBuilder);
    }

    private SelectIndex getSelectIndex(Column column,
                                       Map<String, IndexMeta> indexMap,
                                       Object value) {
        IndexMeta indexMeta = indexMap != null ? indexMap.get(column.getColumnName()) : null;
        if(indexMeta != null) {
            column.setValue(value);
            SelectIndex selectPlan = new SelectIndex();
            selectPlan.setTableName(column.getColumnName());
            selectPlan.setUseIndex(true);
            selectPlan.setIndexType(indexMeta.getIndexType());
            selectPlan.setIndexColumn(column);
            selectPlan.setTableId(indexMeta.getTableId());
            selectPlan.setIndexName(indexMeta.getIndexName());
            return selectPlan;
        }
        return null;
    }

    public String getOperator() {
        return operator;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }


    @Override
    public Expression getJoinCondition(QueryTable mainTable, QueryTable joinTable) {
        if (left instanceof ConditionColumnExpr) {
            ConditionColumnExpr lExp = (ConditionColumnExpr) left;
            if (mainTable.getAlias().equals(lExp.getColumn().getTableAlias())) {
                return this;
            }
        }
        if (right instanceof ConditionColumnExpr) {
            ConditionColumnExpr rExp = (ConditionColumnExpr) right;
            if (mainTable.getAlias().equals(rExp.getColumn().getTableAlias())) {
                return this;
            }
        }
        return super.getJoinCondition(mainTable, joinTable);
    }

    @Override
    public boolean equals(Object o) {

        if(o instanceof SingleComparison) {
            SingleComparison comparison = (SingleComparison) o;
            if (operator.equals(comparison.getOperator())
                    && left.equals(comparison.getLeft())
                    && right.equals(comparison.getRight())) {
                return true;
            }
        }
        return false;
    }



}
