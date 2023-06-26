package com.moyu.test.command.dml.sql;

import com.moyu.test.constant.ConditionConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.cursor.RowEntity;

import java.util.List;

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
     * 判断数据行是否是匹配查询条件
     * @param row
     * @param conditionTree
     * @return
     */
    public static boolean isMatch(RowEntity row, ConditionTree2 conditionTree) {

        if(row.isDeleted()) {
            return false;

        }
        if(conditionTree == null) {
            return true;
        }

        return analyzeConditionTree(conditionTree, row);
    }

    /**
     * 分析条件树，判断行数据是否满足条件
     * @param node
     * @param row
     * @return
     */
    public static boolean analyzeConditionTree(ConditionTree2 node, RowEntity row) {
        boolean result;
        if (node.isLeaf()) {
            result = node.getCondition().getResult(row);
        } else {
            result = true;
            List<ConditionTree2> childNodes = node.getChildNodes();

            for (int i = 0; i < childNodes.size(); i++) {

                ConditionTree2 conditionNode = childNodes.get(i);

                String joinType = conditionNode.getJoinType();
                boolean childResult = analyzeConditionTree(conditionNode, row);
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




}
