package com.moyu.test.command.dml.condition;

import com.moyu.test.constant.ConditionConstant;
import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.exception.SqlQueryException;
import com.moyu.test.store.metadata.obj.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class ConditionComparator {


    public static boolean and(boolean left, boolean right) {
        return left && right;
    }

    public static boolean and(boolean... arr) {
        boolean result = true;
        for (int i = 0; i < arr.length; i++) {
            result = result && arr[i];
        }
        return result;
    }

    public static boolean or(boolean left, boolean right) {
        return left || right;
    }

    public static boolean or(boolean... arr) {
        boolean result = false;
        for (int i = 0; i < arr.length; i++) {
            result = result || arr[i];
        }
        return result;
    }


    /**
     * 分析条件树，判断行数据是否满足条件
     * @param node
     * @param columnData
     * @return
     */
    public static boolean analyzeConditionTree(ConditionTree node, Column[] columnData) {
        boolean result;
        if (node.isLeaf()) {
            result = ConditionComparator.compareCondition(node.getCondition(), columnData);
        } else {
            result = true;
            List<ConditionTree> childNodes = node.getChildNodes();

            for (int i = 0; i < childNodes.size(); i++) {

                ConditionTree conditionNode = childNodes.get(i);

                String joinType = conditionNode.getJoinType();
                boolean childResult = analyzeConditionTree(conditionNode, columnData);
                // 第一个条件
                if (i == 0) {
                    result = childResult;
                    continue;
                }
                // AND条件
                if (ConditionConstant.AND.equals(joinType)) {
                    result = result && childResult;
                    //如果存在一个false，直接返回false.不需要再判断后面条件
                    if (!result) {
                        node.setResult(false);
                        return false;
                    }
                } else if (ConditionConstant.OR.equals(joinType)) {
                    // OR 条件
                    result = result || childResult;
                    if (result) {
                        node.setResult(true);
                        return true;
                    }
                } else {
                    throw new SqlIllegalException("sql条件异常，只支持and或者or");
                }
            }
        }
        node.setResult(result);
        return result;
    }



    public static boolean compareCondition(Condition condition, Column[] columns) {
        boolean result = false;
        List<String> conditionValueList = condition.getValue();

        Map<String, Column> columnMap = new HashMap<>();
        for (Column column : columns) {
            columnMap.put(column.getColumnName(), column);
        }

        String columnName = condition.getKey();
        Column column = columnMap.get(columnName);
        if(column == null) {
            throw new SqlQueryException("字段" + columnName + "不存在");
        }

        Object value = column.getValue();

        switch (condition.getOperator()) {
            case OperatorConstant.EQUAL:
                String value1 = String.valueOf(value);
                if(value1 != null && value1.equals(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
                String value2 = String.valueOf(value);
                if(value2 != null && !value2.equals(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.LIKE:
                String value3 = String.valueOf(value);
                if(value3 != null && value3.contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_LIKE:
                String value4 = String.valueOf(value);
                if(value4 != null && !value4.contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.IN:
                String value5 = String.valueOf(value);
                Set<String> valueSet = conditionValueList.stream().collect(Collectors.toSet());
                if(value != null && valueSet.contains(value5)){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_IN:
                String value6 = String.valueOf(value);
                Set<String> valueSet2 = conditionValueList.stream().collect(Collectors.toSet());
                if(value != null && !valueSet2.contains(value6)){
                    result = true;
                }
                break;
            case OperatorConstant.IS_NULL:
                if(value == null) {
                    result = true;
                }
                break;
            case OperatorConstant.IS_NOT_NULL:
                if(value != null) {
                    result = true;
                }
                break;
            case OperatorConstant.EXISTS:
                // todo
                break;
            case OperatorConstant.NOT_EXISTS:
                // todo
                break;
            default:
                throw new UnsupportedOperationException("不支持操作符:" + condition.getOperator());
        }

        return result;
    }


}
