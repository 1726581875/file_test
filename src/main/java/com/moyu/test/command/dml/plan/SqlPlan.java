package com.moyu.test.command.dml.plan;
import com.moyu.test.command.dml.condition.Condition;
import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.constant.ConditionConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.metadata.obj.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/29
 */
public class SqlPlan {


    public static SelectPlan getSelectPlan(ConditionTree conditionTree, Column[] columns) {

        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            columnMap.put(c.getColumnName(), c);
        }
        return analyzeTree(conditionTree, columnMap);
    }


    private static SelectPlan analyzeTree(ConditionTree conditionTree, Map<String,Column> columnMap) {
        if(conditionTree.isLeaf()) {
            // TODO 目前只处理最简单的情况，按单个主键索引查询
            Condition condition = conditionTree.getCondition();
            String key = condition.getKey();
            Column column = columnMap.get(key);
            if(column == null) {
                throw new SqlIllegalException("字段" + key + "不存在");
            }
            if(column.getIsPrimaryKey() == (byte)1) {
                column.setValue(condition.getValue().get(0));
                SelectPlan selectPlan = new SelectPlan();
                selectPlan.setTableName(column.getColumnName());
                selectPlan.setUseIndex(true);
                selectPlan.setIndexColumn(column);
                // TODO tableId
                selectPlan.setTableId(null);
                return selectPlan;
            }
        } else {
            List<ConditionTree> childNodes = conditionTree.getChildNodes();
            if (childNodes != null) {
                for (int i = 0; i < childNodes.size(); i++) {
                    SelectPlan selectPlan = analyzeTree(childNodes.get(i), columnMap);
                    if(selectPlan != null) {
                        return selectPlan;
                    }
                }
            }
        }
        return null;
    }



}
