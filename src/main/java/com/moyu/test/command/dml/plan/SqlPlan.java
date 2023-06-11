package com.moyu.test.command.dml.plan;
import com.moyu.test.command.dml.condition.Condition;
import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.command.dml.sql.Condition2;
import com.moyu.test.command.dml.sql.ConditionEqOrNq;
import com.moyu.test.command.dml.sql.ConditionTree2;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/29
 */
public class SqlPlan {


    public static SelectPlan getSelectPlan(ConditionTree2 conditionTree, Column[] columns, List<IndexMetadata> indexMetadataList) {

        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            String tableAlias = c.getTableAlias() == null ? "" : c.getTableAlias() + ".";
            columnMap.put(tableAlias + c.getColumnName(), c);
        }

        Map<String, IndexMetadata> indexMap = new HashMap<>();
        if(indexMetadataList != null) {
            for (IndexMetadata idx : indexMetadataList) {
                indexMap.put(idx.getColumnName(), idx);
            }
        }


        return analyzeTree(conditionTree, columnMap, indexMap);
    }


    private static SelectPlan analyzeTree(ConditionTree2 conditionTree,
                                          Map<String,Column> columnMap,
                                          Map<String, IndexMetadata> indexMap) {
        if(conditionTree.isLeaf()) {
            // TODO 目前只处理最简单的情况，按单个索引查询
            Condition2 condition = conditionTree.getCondition();

            if(condition instanceof ConditionEqOrNq) {
                ConditionEqOrNq eqCondition = (ConditionEqOrNq) condition;
                if(eqCondition.isEq()) {
                    Column column = eqCondition.getColumn();
                    IndexMetadata indexMetadata = indexMap.get(column.getColumnName());
                    if(indexMetadata != null) {
                        column.setValue(eqCondition.getValue());
                        SelectPlan selectPlan = new SelectPlan();
                        selectPlan.setTableName(column.getColumnName());
                        selectPlan.setUseIndex(true);
                        selectPlan.setIndexType(indexMetadata.getIndexType());
                        selectPlan.setIndexColumn(column);
                        selectPlan.setTableId(indexMetadata.getTableId());
                        selectPlan.setIndexName(indexMetadata.getIndexName());
                        return selectPlan;
                    }
                }
            }


        } else {
            List<ConditionTree2> childNodes = conditionTree.getChildNodes();
            if (childNodes != null) {
                for (int i = 0; i < childNodes.size(); i++) {
                    SelectPlan selectPlan = analyzeTree(childNodes.get(i), columnMap, indexMap);
                    if(selectPlan != null) {
                        return selectPlan;
                    }
                }
            }
        }
        return null;
    }



}
