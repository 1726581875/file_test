package com.moyu.test.command.dml.sql;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionTree {

    /**
     * 非叶子节点不为空，AND/OR
     */
    private String joinType;
    /**
     * 条件结果
     */
    private boolean result;
    /**
     * 子节点
     */
    private List<ConditionTree> childNodes;
    /**
     * 叶子节点不为null
     */
    private Condition condition;
    /**
     * 是否叶子节点
     */
    private boolean isLeaf;


    public ConditionTree(){}

    public ConditionTree(String joinType, List<ConditionTree> childNodes, boolean isLeaf) {
        this.joinType = joinType;
        this.childNodes = childNodes;
        this.isLeaf = isLeaf;
    }

    public void closeConditionTree() {
        closeCondition(this);
    }


    private void closeCondition(ConditionTree node) {
        if (node.isLeaf()) {
          node.getCondition().close();
        } else {
            List<ConditionTree> childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.size(); i++) {
               closeCondition(childNodes.get(i));
            }
        }
    }


    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public List<ConditionTree> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<ConditionTree> childNodes) {
        this.childNodes = childNodes;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

}
