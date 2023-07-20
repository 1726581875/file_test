package com.moyu.test.command.dml.plan;

import com.moyu.test.command.dml.sql.Condition;
import com.moyu.test.command.dml.sql.ConditionTree;
import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.constant.ConditionConstant;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/29
 */
public class Optimizer {

    private Query originalQuery;

    private Query optimizedQuery;


    public Optimizer(Query originalQuery) {
        this.originalQuery = originalQuery;
        this.optimizedQuery = originalQuery;
    }

    public Query optimizeQuery(){

        return originalQuery;
    }
    /**
     * 进行条件树的裁剪优化
     * @param currNode
     * @return
     */
    private ConditionTree optimizeConditionTree(ConditionTree currNode, ConditionTree parentNode, int currIndex) {

        List<ConditionTree> childNodes = currNode.getChildNodes();
        // 递归处理子树
        if (childNodes != null && childNodes.size() > 0) {
            for (int i = 0; i < childNodes.size(); i++) {
                ConditionTree conditionTree = childNodes.get(i);
                optimizeConditionTree(conditionTree, currNode, i);
            }
        }

        // 如果子节点只有一个，可以直接用子节点代替当前节点，以降低树高度
        if (currNode.getChildNodes() != null && currNode.getChildNodes().size() == 1) {
            currNode = currNode.getChildNodes().get(0);
            parentNode.getChildNodes().set(currIndex, currNode);
        }


        if(currNode.isLeaf()) {

        }

        // 合并等价的条件分支：
/*        if (childNodes != null && childNodes.size() > 0) {
            for (int i = 0; i < childNodes.size(); i++) {
                ConditionTree conditionTree = childNodes.get(i);
                if(conditionTree.isLeaf()) {

                }
            }
        }*/



        return currNode;
    }



    /**
     * 情况1: select * from table where 1 = 1 or id = '1'
     * 情况2: select * from table where 1 = 1 and id = '1'
     * 情况3: select * from table where b=1 and 1 = 1 or id = '1'
     * @param currNode
     * @param parent
     * @param index
     */
    private ConditionTree pruneTree(ConditionTree currNode, ConditionTree parent, int index) {

        Condition condition = currNode.getCondition();
        boolean result = condition.getResult(null);
        List<ConditionTree> childNodes = parent.getChildNodes();

        if (index == 0) {
            if (childNodes.size() > 1) {
                ConditionTree conditionTree = childNodes.get(index + 1);
                if (conditionTree.isLeaf()) {
                    String joinType = conditionTree.getJoinType();
                    if (result && ConditionConstant.OR.equals(joinType)) {

                    }
                }
            } else {

            }
        }

        if (index < childNodes.size() - 1) {
            ConditionTree conditionTree = childNodes.get(index + 1);
            if (conditionTree.isLeaf()) {
                String joinType = conditionTree.getJoinType();
                if (ConditionConstant.OR.equals(joinType)) {

                }
            }
        } else {


        }

        return null;
    }








}
